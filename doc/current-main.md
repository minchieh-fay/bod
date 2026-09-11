# 当前 main：多层网格实现

当前 `main` 延续 `v0.2.0` 的多层双边网格策略，核心代码位于 `main.go`。

## 当前配置

```json
{
  "symbol": "ETHUSDC",
  "quantity": "0.01",
  "spread": "0.0002",
  "layers": 1,
  "check_interval_seconds": 20,
  "reprice_interval_seconds": 600,
  "error_retry_seconds": 60,
  "testnet": false,
  "state_file": "/app/data/state.json"
}
```

`layers` 改为 `2` 时，程序会维护 BUY1/BUY2 和 SELL1/SELL2 四个订单；第 2 层使用 `3 × spread`。

## 当前运行流程

```text
启动
  -> 读取 state.json 并结算上一进程的 current_profit
  -> 查询 ETHUSDC 当前 BOD 多层挂单
  -> 查询已结束的程序订单并记录 FILLED
  -> 保存当前订单列表
  -> 按两侧数量规则创建、转换或重校准网格
  -> 每 20 秒重复
```

程序只识别能解析为 `BOD<n>_...` 的订单，不操作人类订单。程序密钥固定从 `/root/key` 读取，第一行 API Key，第二行 Secret。

## 当前收益字段

```text
total_profit   历史累计收益
current_profit 本次进程启动后的收益
total_trades   历史成交笔数
current_trades 本次进程成交笔数
pending_buys   本次进程未配对的 BUY 成交
pending_sells  本次进程未配对的 SELL 成交
```

运行期间只增加 `current_profit`。程序启动时执行：

```text
total_profit += 上次 current_profit
total_trades += 上次 current_trades
current_profit = 0
current_trades = 0
```

启动时会清空未配对队列，避免跨进程把旧成交和新成交配成一对。

## 重要边界

- 订单只有完整终态 `FILLED` 才进入成交统计。
- 每到 `reprice_interval_seconds` 重校准节点，所有当前程序挂单都会依据最新价格、方向和层号撤单重挂；订单被取消或过期后，网格管理逻辑会根据两侧数量重新补齐或调整。
- 同一方向连续成交不会报错，而是进入对应 FIFO 队列等待另一方向成交。
- 当前代码按账户净仓位和程序订单共同工作，交易所无法区分同一账户中人工持仓与程序持仓。
