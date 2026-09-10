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
	"strings"
	"syscall"
	"time"

	"github.com/adshao/go-binance/v2/futures"
)

const clientOrderPrefix = "BOD_"
const credentialsFile = "/root/key"

type Config struct {
	APIKey                 string `json:"-"`
	APISecret              string `json:"-"`
	Symbol                 string `json:"symbol"`
	Quantity               string `json:"quantity"`
	Spread                 string `json:"spread"`
	CheckIntervalSeconds   int    `json:"check_interval_seconds"`
	RepriceIntervalSeconds int    `json:"reprice_interval_seconds"`
	ErrorRetrySeconds      int    `json:"error_retry_seconds"`
	InitialSide            string `json:"initial_side"`
	Testnet                bool   `json:"testnet"`
	StateFile              string `json:"state_file"`
}

type State struct {
	OrderID       int64  `json:"order_id"`
	ClientOrderID string `json:"client_order_id"`
	Side          string `json:"side"`
	Price         string `json:"price"`
	Quantity      string `json:"quantity"`
	ExecutedQty   string `json:"executed_qty"`
	Status        string `json:"status"`
	UpdatedAt     string `json:"updated_at"`
	TotalProfit   string `json:"total_profit"`
	CurrentProfit string `json:"current_profit"`
	TotalTrades   int    `json:"total_trades"`
	CurrentTrades int    `json:"current_trades"`
}

type StartupTrade struct {
	OrderID  int64
	Side     string
	Price    *big.Float
	Quantity *big.Float
}

type SymbolRules struct {
	TickSize *big.Float
	StepSize *big.Float
	MinQty   *big.Float
}

type Bot struct {
	cfg            Config
	client         *futures.Client
	rules          SymbolRules
	state          State
	statePath      string
	startupTrades  []StartupTrade
	processedFills map[int64]bool
	startupProfit  *big.Float
	startupReprice bool
	lastOrderTime  time.Time
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	configPath := "config.json"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}
	cfg, err := loadConfig(configPath)
	if err != nil {
		log.Fatal(err)
	}

	if cfg.Testnet {
		futures.UseTestnet = true
	}
	client := futures.NewClient(cfg.APIKey, cfg.APISecret)
	bot := &Bot{
		cfg:            cfg,
		client:         client,
		statePath:      cfg.StateFile,
		processedFills: make(map[int64]bool),
		startupProfit:  new(big.Float).SetInt64(0),
	}
	if err := bot.loadState(); err != nil {
		log.Fatal(err)
	}
	bot.startupReprice = bot.state.OrderID != 0

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := bot.loadRules(ctx); err != nil {
		log.Fatal(err)
	}

	log.Printf("BOD started symbol=%s quantity=%s spread=%s testnet=%t", cfg.Symbol, cfg.Quantity, cfg.Spread, cfg.Testnet)
	bot.run(ctx)
}

func loadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config: %w", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("parse config: %w", err)
	}
	cfg.Symbol = strings.ToUpper(strings.TrimSpace(cfg.Symbol))
	cfg.InitialSide = strings.ToUpper(strings.TrimSpace(cfg.InitialSide))
	if cfg.Symbol == "" {
		cfg.Symbol = "ETHUSDC"
	}
	if cfg.Quantity == "" {
		cfg.Quantity = "0.009"
	}
	if cfg.Spread == "" {
		cfg.Spread = "0.0002"
	}
	if cfg.CheckIntervalSeconds <= 0 {
		cfg.CheckIntervalSeconds = 20
	}
	if cfg.RepriceIntervalSeconds <= 0 {
		cfg.RepriceIntervalSeconds = 600
	}
	if cfg.ErrorRetrySeconds <= 0 {
		cfg.ErrorRetrySeconds = 60
	}
	if cfg.InitialSide != "BUY" && cfg.InitialSide != "SELL" {
		return Config{}, errors.New("initial_side must be BUY or SELL")
	}
	if cfg.StateFile == "" {
		cfg.StateFile = "state.json"
	}
	key, secret, keyErr := loadKeyFile(credentialsFile)
	if keyErr != nil {
		return Config{}, fmt.Errorf("cannot load credentials from %s: %w", credentialsFile, keyErr)
	}
	cfg.APIKey = key
	cfg.APISecret = secret
	return cfg, nil
}

func loadKeyFile(path string) (string, string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", "", err
	}
	lines := strings.Fields(string(data))
	if len(lines) < 2 {
		return "", "", fmt.Errorf("key file %s must contain API key and secret on two lines", path)
	}
	return lines[0], lines[1], nil
}

func (b *Bot) loadRules(ctx context.Context) error {
	info, err := b.client.NewExchangeInfoService().Do(ctx)
	if err != nil {
		return fmt.Errorf("load exchange info: %w", err)
	}
	for _, symbol := range info.Symbols {
		if symbol.Symbol != b.cfg.Symbol {
			continue
		}
		if symbol.Status != "TRADING" {
			return fmt.Errorf("symbol %s is not trading: %s", b.cfg.Symbol, symbol.Status)
		}
		var tick, step, minQty string
		for _, f := range symbol.Filters {
			filterType, _ := f["filterType"].(string)
			switch filterType {
			case "PRICE_FILTER":
				tick, _ = f["tickSize"].(string)
			case "LOT_SIZE":
				step, _ = f["stepSize"].(string)
				minQty, _ = f["minQty"].(string)
			}
		}
		var parseErr error
		b.rules.TickSize, parseErr = decimal(tick)
		if parseErr != nil {
			return fmt.Errorf("parse tick size: %w", parseErr)
		}
		b.rules.StepSize, parseErr = decimal(step)
		if parseErr != nil {
			return fmt.Errorf("parse step size: %w", parseErr)
		}
		b.rules.MinQty, parseErr = decimal(minQty)
		if parseErr != nil {
			return fmt.Errorf("parse min qty: %w", parseErr)
		}
		return nil
	}
	return fmt.Errorf("symbol not found: %s", b.cfg.Symbol)
}

func (b *Bot) run(ctx context.Context) {
	interval := time.Duration(b.cfg.CheckIntervalSeconds) * time.Second
	for {
		if err := b.reconcile(ctx); err != nil {
			log.Printf("reconcile failed: %v; retrying in %d seconds", err, b.cfg.ErrorRetrySeconds)
			if !sleepContext(ctx, time.Duration(b.cfg.ErrorRetrySeconds)*time.Second) {
				return
			}
			continue
		}
		if !sleepContext(ctx, interval) {
			return
		}
	}
}

func (b *Bot) reconcile(ctx context.Context) error {
	orders, err := b.client.NewListOpenOrdersService().Symbol(b.cfg.Symbol).Do(ctx)
	if err != nil {
		return fmt.Errorf("list open orders: %w", err)
	}
	var own []*futures.Order
	for _, order := range orders {
		if strings.HasPrefix(order.ClientOrderID, clientOrderPrefix) {
			own = append(own, order)
		}
	}
	if len(own) > 1 {
		return fmt.Errorf("found %d open BOD orders; refusing to create another order", len(own))
	}
	if len(own) == 1 {
		return b.checkOwnOrder(ctx, own[0])
	}

	if b.state.OrderID != 0 {
		// The open-order query is authoritative. Re-query the known order before
		// adopting the account position after a restart or a lost API response.
		order, queryErr := b.client.NewGetOrderService().Symbol(b.cfg.Symbol).OrderID(b.state.OrderID).Do(ctx)
		if queryErr != nil {
			return fmt.Errorf("check known order %d: %w", b.state.OrderID, queryErr)
		}
		if order.Status == futures.OrderStatusTypeNew || order.Status == futures.OrderStatusTypePartiallyFilled {
			return b.checkOwnOrder(ctx, order)
		}
		if order.Status == futures.OrderStatusTypeFilled {
			// The order may have filled between two polling cycles. Use its
			// executed quantity instead of the configured default quantity.
			return b.checkOwnOrder(ctx, order)
		}
		if b.state.Side != "BUY" && b.state.Side != "SELL" {
			return fmt.Errorf("known order %d ended as %s but saved order side is invalid: %q", order.OrderID, order.Status, b.state.Side)
		}
		// Any terminal order other than FILLED is replaced using the current
		// configured full quantity. This also handles partial fills whose
		// remainder would be below the exchange minimum quantity.
		price, priceErr := b.referencePrice(ctx)
		if priceErr != nil {
			return priceErr
		}
		log.Printf("order=%d ended as %s executed=%s; replacing full quantity=%s same side=%s at a new price", order.OrderID, order.Status, order.ExecutedQuantity, b.state.Quantity, b.state.Side)
		return b.placeOrder(ctx, b.state.Side, price)
	}

	// No open BOD order: automatically adopt the current net position.
	position, err := b.currentPosition(ctx)
	if err != nil {
		return err
	}
	side := b.cfg.InitialSide
	if position.Sign() > 0 {
		side = "SELL"
	} else if position.Sign() < 0 {
		side = "BUY"
	}
	price, err := b.referencePrice(ctx)
	if err != nil {
		return err
	}
	if position.Sign() == 0 {
		log.Printf("no BOD order and flat position; using initial side %s", side)
	} else {
		log.Printf("no BOD order; adopting current position %s; first side %s", position.Text('f', -1), side)
	}
	return b.placeOrder(ctx, side, price)
}

func (b *Bot) checkOwnOrder(ctx context.Context, open *futures.Order) error {
	if open.Status == futures.OrderStatusTypeNew || open.Status == futures.OrderStatusTypePartiallyFilled {
		log.Printf("waiting order=%d side=%s status=%s executed=%s/%s", open.OrderID, open.Side, open.Status, open.ExecutedQuantity, open.OrigQuantity)
		previousStats := b.state
		b.state = stateFromOrder(open)
		b.state.TotalProfit = previousStats.TotalProfit
		b.state.CurrentProfit = previousStats.CurrentProfit
		b.state.TotalTrades = previousStats.TotalTrades
		b.state.CurrentTrades = previousStats.CurrentTrades
		if open.Time > 0 {
			b.lastOrderTime = time.UnixMilli(open.Time)
		}
		if err := b.saveState(); err != nil {
			return err
		}
		if open.Status == futures.OrderStatusTypeNew && b.shouldReprice() {
			return b.repriceOrder(ctx, open)
		}
		b.startupReprice = false
		return nil
	}
	if open.Status != futures.OrderStatusTypeFilled {
		return fmt.Errorf("BOD order %d has unexpected status %s", open.OrderID, open.Status)
	}

	qty, err := b.normalizeQuantity(open.ExecutedQuantity)
	if err != nil {
		return fmt.Errorf("normalize executed quantity: %w", err)
	}
	if qty.Sign() <= 0 {
		return fmt.Errorf("order %d is FILLED but executed quantity is zero", open.OrderID)
	}
	if err := b.recordStartupTrade(open, qty); err != nil {
		return err
	}
	side := "SELL"
	if open.Side == "SELL" {
		side = "BUY"
	}
	price, err := b.referencePrice(ctx)
	if err != nil {
		return err
	}
	log.Printf("order filled order=%d; placing reverse side=%s quantity=%s", open.OrderID, side, qty.Text('f', -1))
	return b.placeOrder(ctx, side, price)
}

func (b *Bot) recordStartupTrade(order *futures.Order, quantity *big.Float) error {
	if b.processedFills[order.OrderID] {
		return nil
	}
	priceText := order.AvgPrice
	if priceText == "" || priceText == "0" {
		priceText = order.Price
	}
	price, err := decimal(priceText)
	if err != nil || price.Sign() <= 0 {
		return fmt.Errorf("invalid filled price for order %d: %q", order.OrderID, priceText)
	}
	b.processedFills[order.OrderID] = true
	b.startupTrades = append(b.startupTrades, StartupTrade{
		OrderID:  order.OrderID,
		Side:     string(order.Side),
		Price:    price,
		Quantity: new(big.Float).Copy(quantity),
	})

	if len(b.startupTrades)%2 != 0 {
		b.state.CurrentTrades = len(b.startupTrades)
		b.state.CurrentProfit = b.startupProfit.Text('f', 8)
		if err := b.saveState(); err != nil {
			return fmt.Errorf("save trade statistics: %w", err)
		}
		b.logProfit()
		return nil
	}
	first := b.startupTrades[len(b.startupTrades)-2]
	second := b.startupTrades[len(b.startupTrades)-1]
	if first.Side == second.Side {
		return fmt.Errorf("two consecutive filled BOD orders have the same side: %s", first.Side)
	}
	matchedQty := first.Quantity
	if second.Quantity.Cmp(matchedQty) < 0 {
		matchedQty = second.Quantity
	}
	profit := new(big.Float)
	if first.Side == "BUY" {
		profit.Sub(second.Price, first.Price)
	} else {
		profit.Sub(first.Price, second.Price)
	}
	profit.Mul(profit, matchedQty)
	b.startupProfit.Add(b.startupProfit, profit)
	b.state.CurrentTrades = len(b.startupTrades)
	b.state.CurrentProfit = b.startupProfit.Text('f', 8)
	if err := b.saveState(); err != nil {
		return fmt.Errorf("save trade statistics: %w", err)
	}
	b.logProfit()
	return nil
}

func (b *Bot) logProfit() {
	log.Printf("时间=%s 交易次数=%d 总收益=%s USDC 当前启动收益=%s USDC", time.Now().Format(time.RFC3339), b.state.CurrentTrades, b.state.TotalProfit, b.state.CurrentProfit)
}

func (b *Bot) placeOrder(ctx context.Context, side, referencePrice string, quantities ...string) error {
	quantity := b.cfg.Quantity
	if len(quantities) == 1 {
		quantity = quantities[0]
	}
	qty, err := b.normalizeQuantity(quantity)
	if err != nil {
		return fmt.Errorf("normalize quantity: %w", err)
	}
	price, err := b.calculatePrice(side, referencePrice)
	if err != nil {
		return err
	}
	qtyText := qty.Text('f', -1)
	priceText := price.Text('f', -1)
	clientOrderID := fmt.Sprintf("%s%d", clientOrderPrefix, time.Now().UnixNano())

	order, err := b.client.NewCreateOrderService().
		Symbol(b.cfg.Symbol).
		Side(futures.SideType(side)).
		Type(futures.OrderTypeLimit).
		TimeInForce(futures.TimeInForceTypeGTC).
		Quantity(qtyText).
		Price(priceText).
		NewClientOrderID(clientOrderID).
		Do(ctx)
	if err != nil {
		return fmt.Errorf("create %s order: %w", side, err)
	}
	previousStats := b.state
	b.state = State{
		OrderID: order.OrderID, ClientOrderID: order.ClientOrderID, Side: side,
		Price: priceText, Quantity: qtyText, ExecutedQty: order.ExecutedQuantity,
		Status: string(order.Status), UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		TotalProfit: previousStats.TotalProfit, CurrentProfit: previousStats.CurrentProfit,
		TotalTrades: previousStats.TotalTrades, CurrentTrades: previousStats.CurrentTrades,
	}
	b.lastOrderTime = time.Now()
	b.startupReprice = false
	if err := b.saveState(); err != nil {
		return fmt.Errorf("save order state: %w", err)
	}
	log.Printf("created order=%d client_order_id=%s side=%s quantity=%s price=%s", order.OrderID, clientOrderID, side, qtyText, priceText)
	return nil
}

func (b *Bot) calculatePrice(side, referencePrice string) (*big.Float, error) {
	spread, err := decimal(b.cfg.Spread)
	if err != nil || spread.Sign() < 0 {
		return nil, fmt.Errorf("invalid spread %q", b.cfg.Spread)
	}
	ref, err := decimal(referencePrice)
	if err != nil || ref.Sign() <= 0 {
		return nil, fmt.Errorf("invalid reference price %q", referencePrice)
	}
	multiplier := new(big.Float).SetFloat64(1)
	if side == "BUY" {
		multiplier.Sub(multiplier, spread)
	} else {
		multiplier.Add(multiplier, spread)
	}
	price := floorToStep(new(big.Float).Mul(ref, multiplier), b.rules.TickSize)
	if price.Sign() <= 0 {
		return nil, errors.New("calculated price is not positive")
	}
	return price, nil
}

func (b *Bot) shouldReprice() bool {
	return b.startupReprice || b.lastOrderTime.IsZero() || time.Since(b.lastOrderTime) >= time.Duration(b.cfg.RepriceIntervalSeconds)*time.Second
}

func (b *Bot) repriceOrder(ctx context.Context, open *futures.Order) error {
	reference, err := b.referencePrice(ctx)
	if err != nil {
		return err
	}
	newPrice, err := b.calculatePrice(string(open.Side), reference)
	if err != nil {
		return err
	}
	oldPrice, err := decimal(open.Price)
	if err != nil {
		return fmt.Errorf("invalid existing order price %q: %w", open.Price, err)
	}
	closer := (open.Side == futures.SideTypeBuy && newPrice.Cmp(oldPrice) > 0) ||
		(open.Side == futures.SideTypeSell && newPrice.Cmp(oldPrice) < 0)
	b.startupReprice = false
	b.lastOrderTime = time.Now()
	if !closer {
		log.Printf("order=%d reprice checked; keeping price=%s new_price=%s", open.OrderID, open.Price, newPrice.Text('f', -1))
		return nil
	}
	if _, err := b.client.NewCancelOrderService().Symbol(b.cfg.Symbol).OrderID(open.OrderID).Do(ctx); err != nil {
		return fmt.Errorf("cancel order %d for reprice: %w", open.OrderID, err)
	}
	log.Printf("order=%d reprice old_price=%s new_price=%s", open.OrderID, open.Price, newPrice.Text('f', -1))
	return b.placeOrder(ctx, string(open.Side), reference)
}

func (b *Bot) currentPosition(ctx context.Context) (*big.Float, error) {
	positions, err := b.client.NewGetPositionRiskService().Symbol(b.cfg.Symbol).Do(ctx)
	if err != nil {
		return nil, fmt.Errorf("get position: %w", err)
	}
	total := new(big.Float).SetInt64(0)
	found := false
	for _, position := range positions {
		if position.Symbol != b.cfg.Symbol {
			continue
		}
		found = true
		value, parseErr := decimal(position.PositionAmt)
		if parseErr != nil {
			return nil, fmt.Errorf("invalid position amount %q: %w", position.PositionAmt, parseErr)
		}
		total.Add(total, value)
	}
	if found {
		return total, nil
	}
	return nil, fmt.Errorf("position for %s not found", b.cfg.Symbol)
}

func (b *Bot) referencePrice(ctx context.Context) (string, error) {
	prices, err := b.client.NewListPricesService().Symbol(b.cfg.Symbol).Do(ctx)
	if err != nil {
		return "", fmt.Errorf("get mark price: %w", err)
	}
	if len(prices) != 1 {
		return "", fmt.Errorf("unexpected mark price response count: %d", len(prices))
	}
	return prices[0].Price, nil
}

func (b *Bot) normalizeQuantity(value string) (*big.Float, error) {
	qty, err := decimal(value)
	if err != nil {
		return nil, err
	}
	qty = floorToStep(qty, b.rules.StepSize)
	if qty.Cmp(b.rules.MinQty) < 0 {
		return nil, fmt.Errorf("quantity %s is below min quantity %s", qty.Text('f', -1), b.rules.MinQty.Text('f', -1))
	}
	return qty, nil
}

func decimal(value string) (*big.Float, error) {
	result, _, err := big.ParseFloat(strings.TrimSpace(value), 10, 256, big.ToNearestEven)
	if err != nil {
		return nil, fmt.Errorf("invalid decimal %q: %w", value, err)
	}
	return result, nil
}

func floorToStep(value, step *big.Float) *big.Float {
	quotient := new(big.Float).Quo(value, step)
	integer, _ := quotient.Int(nil)
	return new(big.Float).Mul(new(big.Float).SetInt(integer), step)
}

func stateFromOrder(order *futures.Order) State {
	return State{OrderID: order.OrderID, ClientOrderID: order.ClientOrderID, Side: string(order.Side), Price: order.Price, Quantity: order.OrigQuantity, ExecutedQty: order.ExecutedQuantity, Status: string(order.Status), UpdatedAt: time.Now().UTC().Format(time.RFC3339)}
}

func (b *Bot) loadState() error {
	data, err := os.ReadFile(b.statePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("read state: %w", err)
	}
	if err := json.Unmarshal(data, &b.state); err != nil {
		return fmt.Errorf("parse state: %w", err)
	}
	if b.state.TotalProfit == "" {
		b.state.TotalProfit = "0"
	}
	if b.state.CurrentProfit == "" {
		b.state.CurrentProfit = "0"
	}
	totalProfit, err := decimal(b.state.TotalProfit)
	if err != nil {
		return fmt.Errorf("parse total profit: %w", err)
	}
	currentProfit, err := decimal(b.state.CurrentProfit)
	if err != nil {
		return fmt.Errorf("parse current profit: %w", err)
	}
	// Commit the previous process session to the lifetime total, then start
	// a fresh current session while preserving the order recovery fields.
	totalProfit.Add(totalProfit, currentProfit)
	b.state.TotalProfit = totalProfit.Text('f', 8)
	b.state.CurrentProfit = "0.00000000"
	b.state.TotalTrades += b.state.CurrentTrades
	b.state.CurrentTrades = 0
	b.startupProfit = new(big.Float).SetInt64(0)
	b.startupTrades = nil
	if err := b.saveState(); err != nil {
		return fmt.Errorf("roll startup statistics: %w", err)
	}
	return nil
}

func (b *Bot) saveState() error {
	data, err := json.MarshalIndent(b.state, "", "  ")
	if err != nil {
		return err
	}
	if dir := filepath.Dir(b.statePath); dir != "." {
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
	}
	tmp := b.statePath + ".tmp"
	if err := os.WriteFile(tmp, data, 0600); err != nil {
		return err
	}
	return os.Rename(tmp, b.statePath)
}

func sleepContext(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
