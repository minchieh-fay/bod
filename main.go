package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

const credentialsFile = "/root/key"
const orderTag = "bod"

type Config struct {
	Symbol                 string `json:"symbol"`
	Quantity               string `json:"quantity"`
	Spread                 string `json:"spread"`
	Layers                 int    `json:"layers"`
	CheckIntervalSeconds   int    `json:"check_interval_seconds"`
	RepriceIntervalSeconds int    `json:"reprice_interval_seconds"`
	ErrorRetrySeconds      int    `json:"error_retry_seconds"`
	Testnet                bool   `json:"testnet"`
	StateFile              string `json:"state_file"`
}

type ManagedOrder struct {
	OrderID       int64  `json:"order_id"`
	ClientOrderID string `json:"client_order_id"`
	Side          string `json:"side"`
	Layer         int    `json:"layer"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	Status        string `json:"status"`
}

type Fill struct {
	Side     string `json:"side"`
	Price    string `json:"price"`
	Quantity string `json:"quantity"`
}
type State struct {
	Orders        []ManagedOrder `json:"orders"`
	PendingBuys   []Fill         `json:"pending_buys"`
	PendingSells  []Fill         `json:"pending_sells"`
	TotalProfit   string         `json:"total_profit"`
	CurrentProfit string         `json:"current_profit"`
	TotalTrades   int            `json:"total_trades"`
	CurrentTrades int            `json:"current_trades"`
}
type Rules struct{ Tick, Step, MinQty *big.Float }
type Bot struct {
	cfg         Config
	client      *futures.Client
	rules       Rules
	state       State
	statePath   string
	lastReprice time.Time
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)
	path := "config.json"
	if len(os.Args) > 1 {
		path = os.Args[1]
	}
	cfg, err := loadConfig(path)
	if err != nil {
		log.Fatal(err)
	}
	if cfg.Testnet {
		futures.UseTestnet = true
	}
	key, secret, err := loadCredentials()
	if err != nil {
		log.Fatal(err)
	}
	b := &Bot{cfg: cfg, client: futures.NewClient(key, secret), statePath: cfg.StateFile}
	if err = b.loadState(); err != nil {
		log.Fatal(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err = b.loadRules(ctx); err != nil {
		log.Fatal(err)
	}
	log.Printf("BOD grid started symbol=%s quantity=%s spread=%s layers=%d check=%ds reprice=%ds testnet=%t", cfg.Symbol, cfg.Quantity, cfg.Spread, cfg.Layers, cfg.CheckIntervalSeconds, cfg.RepriceIntervalSeconds, cfg.Testnet)
	b.run(ctx)
}

func loadConfig(path string) (Config, error) {
	data, e := os.ReadFile(path)
	if e != nil {
		return Config{}, e
	}
	var c Config
	if e = json.Unmarshal(data, &c); e != nil {
		return Config{}, e
	}
	c.Symbol = strings.ToUpper(strings.TrimSpace(c.Symbol))
	if c.Symbol == "" {
		c.Symbol = "ETHUSDC"
	}
	if c.Quantity == "" {
		c.Quantity = "0.01"
	}
	if c.Spread == "" {
		c.Spread = "0.0003"
	}
	if c.Layers <= 0 {
		c.Layers = 1
	}
	if c.CheckIntervalSeconds <= 0 {
		c.CheckIntervalSeconds = 20
	}
	if c.RepriceIntervalSeconds <= 0 {
		c.RepriceIntervalSeconds = 600
	}
	if c.ErrorRetrySeconds <= 0 {
		c.ErrorRetrySeconds = 60
	}
	if c.StateFile == "" {
		c.StateFile = "/app/data/state.json"
	}
	return c, nil
}
func loadCredentials() (string, string, error) {
	d, e := os.ReadFile(credentialsFile)
	if e != nil {
		return "", "", e
	}
	p := strings.Fields(string(d))
	if len(p) < 2 {
		return "", "", errors.New("/root/key must contain API key and secret")
	}
	return p[0], p[1], nil
}

func (b *Bot) loadRules(ctx context.Context) error {
	info, e := b.client.NewExchangeInfoService().Do(ctx)
	if e != nil {
		return e
	}
	for _, s := range info.Symbols {
		if s.Symbol != b.cfg.Symbol {
			continue
		}
		var tick, step, min string
		for _, f := range s.Filters {
			t, _ := f["filterType"].(string)
			if t == "PRICE_FILTER" {
				tick, _ = f["tickSize"].(string)
			}
			if t == "LOT_SIZE" {
				step, _ = f["stepSize"].(string)
				min, _ = f["minQty"].(string)
			}
		}
		b.rules.Tick, e = decimal(tick)
		if e != nil {
			return e
		}
		b.rules.Step, e = decimal(step)
		if e != nil {
			return e
		}
		b.rules.MinQty, e = decimal(min)
		return e
	}
	return fmt.Errorf("symbol not found: %s", b.cfg.Symbol)
}
func (b *Bot) run(ctx context.Context) {
	for {
		if e := b.reconcile(ctx); e != nil {
			log.Printf("reconcile failed: %v; retry in %ds", e, b.cfg.ErrorRetrySeconds)
			if !sleep(ctx, time.Duration(b.cfg.ErrorRetrySeconds)*time.Second) {
				return
			}
			continue
		}
		if !sleep(ctx, time.Duration(b.cfg.CheckIntervalSeconds)*time.Second) {
			return
		}
	}
}

func (b *Bot) reconcile(ctx context.Context) error {
	list, e := b.client.NewListOpenOrdersService().Symbol(b.cfg.Symbol).Do(ctx)
	if e != nil {
		return e
	}
	actual := map[int64]*futures.Order{}
	for _, o := range list {
		if b.parseLayer(o.ClientOrderID) > 0 {
			actual[o.OrderID] = o
		}
	}
	if e = b.processFinished(ctx, actual); e != nil {
		return e
	}
	b.state.Orders = nil
	for _, o := range actual {
		n := b.parseLayer(o.ClientOrderID)
		if n > 0 {
			b.state.Orders = append(b.state.Orders, ManagedOrder{o.OrderID, o.ClientOrderID, string(o.Side), n, o.Price, o.OrigQuantity, string(o.Status)})
		}
	}
	if e = b.saveState(); e != nil {
		return e
	}
	return b.manageGrid(ctx)
}
func (b *Bot) processFinished(ctx context.Context, actual map[int64]*futures.Order) error {
	for _, old := range append([]ManagedOrder(nil), b.state.Orders...) {
		if _, ok := actual[old.OrderID]; ok {
			continue
		}
		o, e := b.client.NewGetOrderService().Symbol(b.cfg.Symbol).OrderID(old.OrderID).Do(ctx)
		if e != nil {
			return e
		}
		if o.Status == futures.OrderStatusTypeFilled {
			if e = b.recordFill(o); e != nil {
				return e
			}
		} else {
			log.Printf("order=%d ended status=%s executed=%s", o.OrderID, o.Status, o.ExecutedQuantity)
		}
	}
	return nil
}

func (b *Bot) manageGrid(ctx context.Context) error {
	buys, sells := b.sideOrders()
	if len(b.state.Orders) == 0 {
		return b.createGrid(ctx)
	}
	if len(buys) == len(sells) && len(buys) < b.cfg.Layers {
		return b.rebuildGrid(ctx, "equal but incomplete sides")
	}
	if b.lastReprice.IsZero() || time.Since(b.lastReprice) >= time.Duration(b.cfg.RepriceIntervalSeconds)*time.Second {
		return b.repriceAll(ctx)
	}
	if len(buys) == 0 || len(sells) == 0 {
		return b.normalizeSingleSide(ctx, buys, sells)
	}
	return nil
}
func (b *Bot) createGrid(ctx context.Context) error {
	ref, e := b.referencePrice(ctx)
	if e != nil {
		return e
	}
	for i := 1; i <= b.cfg.Layers; i++ {
		if e = b.placeLayer(ctx, "BUY", i, ref, ""); e != nil {
			return e
		}
		if e = b.placeLayer(ctx, "SELL", i, ref, ""); e != nil {
			return e
		}
	}
	b.lastReprice = time.Now()
	return nil
}
func (b *Bot) rebuildGrid(ctx context.Context, reason string) error {
	log.Printf("rebuilding full grid: %s", reason)
	if e := b.cancelAll(ctx); e != nil {
		return e
	}
	b.state.Orders = nil
	return b.createGrid(ctx)
}
func (b *Bot) normalizeSingleSide(ctx context.Context, buys, sells []ManagedOrder) error {
	side, orders := "SELL", sells
	if len(buys) > 0 {
		side, orders = "BUY", buys
	}
	var cancel []ManagedOrder
	for _, o := range orders {
		if o.Layer != 1 {
			cancel = append(cancel, o)
		}
	}
	if len(cancel) == 0 {
		return nil
	}
	for _, o := range cancel {
		if _, e := b.client.NewCancelOrderService().Symbol(b.cfg.Symbol).OrderID(o.OrderID).Do(ctx); e != nil {
			return e
		}
	}
	ref, e := b.referencePrice(ctx)
	if e != nil {
		return e
	}
	for _, o := range cancel {
		if e = b.placeLayer(ctx, side, 1, ref, o.Quantity); e != nil {
			return e
		}
	}
	b.lastReprice = time.Now()
	return nil
}

// Reprice every remaining managed order at the scheduled recalibration point.
func (b *Bot) repriceAll(ctx context.Context) error {
	ref, e := b.referencePrice(ctx)
	if e != nil {
		return e
	}
	orders := append([]ManagedOrder(nil), b.state.Orders...)
	log.Printf("repricing all managed orders count=%d reference=%s", len(orders), ref)
	for _, o := range orders {
		if _, e = b.client.NewCancelOrderService().Symbol(b.cfg.Symbol).OrderID(o.OrderID).Do(ctx); e != nil {
			return e
		}
	}
	for _, o := range orders {
		if e = b.placeLayer(ctx, o.Side, o.Layer, ref, o.Quantity); e != nil {
			return e
		}
	}
	b.lastReprice = time.Now()
	return nil
}
func (b *Bot) cancelAll(ctx context.Context) error {
	for _, o := range b.state.Orders {
		if _, e := b.client.NewCancelOrderService().Symbol(b.cfg.Symbol).OrderID(o.OrderID).Do(ctx); e != nil {
			return e
		}
	}
	return nil
}

// placeLayer 按指定层级和数量创建限价单。
func (b *Bot) placeLayer(
	ctx context.Context,
	side string,
	layer int,
	reference string,
	quantity string,
) error {
	// step.1 确定新订单数量；调整层和重新定价时沿用原订单数量
	var qty *big.Float
	var e error
	if quantity == "" {
		qty, e = b.layerQuantity(layer)
	} else {
		qty, e = b.normalize(quantity)
	}
	if e != nil {
		return e
	}
	// step.2 根据参考价、价差和层级计算限价价格
	ref, e := decimal(reference)
	if e != nil {
		return e
	}
	sp, e := decimal(b.cfg.Spread)
	if e != nil {
		return e
	}
	mult := new(big.Float).SetInt64(int64(3*layer - 2))
	offset := new(big.Float).Mul(sp, mult)
	factor := new(big.Float).SetFloat64(1)
	if side == "BUY" {
		factor.Sub(factor, offset)
	} else {
		factor.Add(factor, offset)
	}
	price := floor(new(big.Float).Mul(ref, factor), b.rules.Tick)
	if price.Sign() <= 0 {
		return errors.New("calculated price is not positive")
	}
	// step.3 提交交易所订单并保存本地订单状态
	// Binance requires ETHUSDC prices to have at most two decimal places.
	qt, pt := qty.Text('f', 3), price.Text('f', 2)
	id := fmt.Sprintf("%s%d_%d", b.orderIDPrefix(), layer, time.Now().UnixNano())
	o, e := b.client.NewCreateOrderService().Symbol(b.cfg.Symbol).Side(futures.SideType(side)).Type(futures.OrderTypeLimit).TimeInForce(futures.TimeInForceTypeGTC).Quantity(qt).Price(pt).NewClientOrderID(id).Do(ctx)
	if e != nil {
		return e
	}
	b.state.Orders = append(b.state.Orders, ManagedOrder{o.OrderID, id, side, layer, pt, qt, string(o.Status)})
	return b.saveState()
}

// layerQuantity 计算新建订单在指定层级的买入数量。
func (b *Bot) layerQuantity(layer int) (*big.Float, error) {
	// step.1 读取并校准基础下单数量
	qty, e := b.normalize(b.cfg.Quantity)
	if e != nil {
		return nil, e
	}
	// step.2 按每层增加前一层 50% 的规则计算数量
	ratio := new(big.Float).Quo(big.NewFloat(3), big.NewFloat(2))
	for current := 1; current < layer; current++ {
		qty.Mul(qty, ratio)
	}
	// step.3 按交易所数量步长向下取整并校验最小数量
	return b.normalize(qty.Text('f', -1))
}

func (b *Bot) recordFill(o *futures.Order) error {
	price := o.AvgPrice
	if price == "" || price == "0" {
		price = o.Price
	}
	q, e := b.normalize(o.ExecutedQuantity)
	if e != nil {
		return e
	}
	b.state.CurrentTrades++
	fill := Fill{string(o.Side), price, q.Text('f', -1)}
	if fill.Side == "BUY" {
		b.state.PendingBuys = append(b.state.PendingBuys, fill)
	} else {
		b.state.PendingSells = append(b.state.PendingSells, fill)
	}
	for len(b.state.PendingBuys) > 0 && len(b.state.PendingSells) > 0 {
		buy, sell := b.state.PendingBuys[0], b.state.PendingSells[0]
		bp, _ := decimal(buy.Price)
		sp, _ := decimal(sell.Price)
		bq, _ := decimal(buy.Quantity)
		sq, _ := decimal(sell.Quantity)
		matched := bq
		if sq.Cmp(matched) < 0 {
			matched = sq
		}
		profit := new(big.Float).Sub(sp, bp)
		profit.Mul(profit, matched)
		cur, _ := decimal(b.state.CurrentProfit)
		cur.Add(cur, profit)
		b.state.CurrentProfit = cur.Text('f', 8)
		b.state.PendingBuys[0].Quantity = new(big.Float).Sub(bq, matched).Text('f', -1)
		b.state.PendingSells[0].Quantity = new(big.Float).Sub(sq, matched).Text('f', -1)
		if zero(b.state.PendingBuys[0].Quantity) {
			b.state.PendingBuys = b.state.PendingBuys[1:]
		}
		if zero(b.state.PendingSells[0].Quantity) {
			b.state.PendingSells = b.state.PendingSells[1:]
		}
		log.Printf("时间=%s 交易次数=%d 总收益=%s USDC 当前启动收益=%s USDC", time.Now().Format(time.RFC3339), b.state.CurrentTrades, b.state.TotalProfit, b.state.CurrentProfit)
	}
	return b.saveState()
}

func zero(value string) bool { f, err := decimal(value); return err != nil || f.Sign() == 0 }

func (b *Bot) sideOrders() ([]ManagedOrder, []ManagedOrder) {
	var buy, sell []ManagedOrder
	for _, o := range b.state.Orders {
		if o.Side == "BUY" {
			buy = append(buy, o)
		} else if o.Side == "SELL" {
			sell = append(sell, o)
		}
	}
	sort.Slice(buy, func(i, j int) bool { return buy[i].Layer < buy[j].Layer })
	sort.Slice(sell, func(i, j int) bool { return sell[i].Layer < sell[j].Layer })
	return buy, sell
}
func (b *Bot) referencePrice(ctx context.Context) (string, error) {
	p, e := b.client.NewListPricesService().Symbol(b.cfg.Symbol).Do(ctx)
	if e != nil {
		return "", e
	}
	if len(p) != 1 {
		return "", errors.New("unexpected price response")
	}
	return p[0].Price, nil
}
func (b *Bot) normalize(v string) (*big.Float, error) {
	q, e := decimal(v)
	if e != nil {
		return nil, e
	}
	q = floor(q, b.rules.Step)
	if q.Cmp(b.rules.MinQty) < 0 {
		return nil, fmt.Errorf("quantity below minimum: %s", q.Text('f', -1))
	}
	return q, nil
}
func decimal(v string) (*big.Float, error) {
	f, _, e := big.ParseFloat(strings.TrimSpace(v), 10, 256, big.ToNearestEven)
	if e != nil {
		return nil, e
	}
	return f, nil
}
func floor(v, s *big.Float) *big.Float {
	q := new(big.Float).Quo(v, s)
	i, _ := q.Int(nil)
	return new(big.Float).Mul(new(big.Float).SetInt(i), s)
}

// parseLayer 解析当前币种订单标识中的挂单层级。
func (b *Bot) parseLayer(id string) int {
	p := strings.SplitN(id, "_", 2)
	prefix := b.orderIDPrefix()
	if len(p) != 2 || !strings.HasPrefix(p[0], prefix) {
		return 0
	}
	n, e := strconv.Atoi(strings.TrimPrefix(p[0], prefix))
	if e != nil {
		return 0
	}
	return n
}

// orderIDPrefix 根据交易对生成带基础币种和程序标记的订单前缀。
func (b *Bot) orderIDPrefix() string {
	symbol := strings.ToUpper(b.cfg.Symbol)
	for _, quote := range []string{"USDT", "USDC", "BUSD", "FDUSD", "TUSD", "USD"} {
		if strings.HasSuffix(symbol, quote) && len(symbol) > len(quote) {
			symbol = strings.TrimSuffix(symbol, quote)
			break
		}
	}
	return strings.ToLower(symbol) + orderTag
}

func (b *Bot) loadState() error {
	d, e := os.ReadFile(b.statePath)
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	if e = json.Unmarshal(d, &b.state); e != nil {
		return e
	}
	if b.state.TotalProfit == "" {
		b.state.TotalProfit = "0"
	}
	if b.state.CurrentProfit == "" {
		b.state.CurrentProfit = "0"
	}
	total, e := decimal(b.state.TotalProfit)
	if e != nil {
		return e
	}
	cur, e := decimal(b.state.CurrentProfit)
	if e != nil {
		return e
	}
	total.Add(total, cur)
	b.state.TotalProfit = total.Text('f', 8)
	b.state.CurrentProfit = "0.00000000"
	b.state.TotalTrades += b.state.CurrentTrades
	b.state.CurrentTrades = 0
	b.state.PendingBuys = nil
	b.state.PendingSells = nil
	return b.saveState()
}
func (b *Bot) saveState() error {
	d, e := json.MarshalIndent(b.state, "", "  ")
	if e != nil {
		return e
	}
	if dir := filepath.Dir(b.statePath); dir != "." {
		if e = os.MkdirAll(dir, 0755); e != nil {
			return e
		}
	}
	tmp := b.statePath + ".tmp"
	if e = os.WriteFile(tmp, d, 0600); e != nil {
		return e
	}
	return os.Rename(tmp, b.statePath)
}
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
