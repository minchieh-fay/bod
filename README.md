# BOD ETHUSDC 自动挂单程序

程序只管理 `clientOrderId` 以 `BOD_` 开头的订单，人类订单不会被撤销或修改。

## 运行

1. 复制 `config.example.json` 为 `config.json`。将 API Key 放在服务器 `/root/key` 第一行，将 Secret 放在第二行；Compose 会以只读方式挂载该文件。
2. 默认 `testnet: true`，确认测试网运行正常后再改为 `false`。
3. 启动：

```bash
go run . config.json
```

也可以构建后运行：

```bash
go build -o bod .
./bod config.json
```

## 规则

- 默认下单数量是 `0.009` ETH，只有订单状态为 `FILLED` 才会挂反向单。
- `PARTIALLY_FILLED` 会继续等待，不会挂反向单。
- 当前挂单不会因配置数量变化而修改；订单结束后，下一笔新订单统一使用配置文件当前的 `quantity`。订单最终为 `CANCELED`、`EXPIRED` 或其他非 `FILLED` 状态时，也按原方向和当前配置的完整数量重新挂单。
- 启动时优先恢复当前未完成的 `BOD_` 订单；没有时根据当前 ETHUSDC 净持仓决定方向；空仓时使用 `initial_side`。
- API 错误、持仓无法读取、发现多个程序挂单等情况只暂停新增下单，60 秒后自动重新同步。
- `state.json` 用于保存最近的程序订单状态，建议持久化到容器卷。
- 盈利统计从本次进程启动开始；每个完整成交订单只统计一次，按相邻的 BUY/SELL 两笔成对计算。奇数笔最后一笔只展示交易次数，不计入总收益。日志格式为 `时间=... 交易次数=... 总收益=... USDC`。
- `state.json` 中的 `total_profit` 是历史累计总收益，`current_profit` 是本次启动后的收益；运行期间只更新 `current_profit`，程序启动时才会把上次的 `current_profit` 并入 `total_profit`，然后将 `current_profit` 清零。
- `check_interval_seconds` 控制成交检查间隔，`reprice_interval_seconds` 控制所有现有程序挂单的价格重新校准间隔，默认分别为 20 秒和 600 秒。启动时如果已有程序挂单，会立即校准一次。
- `layers` 控制挂单层数，默认 `1`。第 `n` 层的振幅倍数为 `3*n-2`，例如 `1A`、`4A`、`7A`。第 `n` 层数量为基础数量的 `1.5^(n-1)`，例如 `1`、`1.5`、`2.25`。每一层使用独立的 `BOD<n>_...` 订单标识；单边持仓调整层级时沿用被调整原订单的数量。
- 成交收益按 BUY 和 SELL 两个 FIFO 队列配对；连续单边成交会暂存，未配对成交不计入收益。程序重启时会清空未配对队列，避免跨启动周期配对。

程序会自动接管启动时账户的 ETHUSDC 净持仓。因此单向持仓模式下，程序无法从交易所区分人工持仓和程序持仓；如果人工同时操作同一交易对，程序会按最新净持仓继续运行。
