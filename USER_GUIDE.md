# Alpha Miner 用户一页手册

> 面向“拿系统分析股票并辅助次日操作”的使用者。技术实现请不要从这里展开。

## 当前能不能用

能用于盘后研究和生成观察名单；截至 2026-08-14，涨停候选因子尚未通过实盘闸门，不能据此买入。
先运行下面这条命令，看到“已通过”前都保持 0 仓位：

```powershell
uv run python -m cli zt status
```

## 每天只运行这一条

交易日 15:40 后，在项目目录运行：

```powershell
uv run python -m cli zt daily
```

它依次完成当日数据采集、因子计算和次日操作卡。若数据已经由定时任务更新：

```powershell
uv run python -m cli zt daily --skip-collect
```

只重新查看操作卡、不更新数据：

```powershell
uv run python -m cli zt scan
```

## 我想主动跑一次因子挖掘

日常使用并不需要每天进化。推荐按下面顺序运行：

1. 先更新数据并查看是否达到最低样本量：

   ```powershell
   uv run python -m cli zt daily
   uv run python -m cli zt status
   ```

2. 涨停专项挖掘（推荐）：

   ```powershell
   uv run python -m cli zt evolve --generations 5 --population 24
   ```

   使用计算服务器时，把这一步替换为：

   ```powershell
   # 仅首次：复制模板后填写私有 SSH 用户、服务器地址和远程目录
   Copy-Item config\remote.example.ps1 config\remote.local.ps1

   .\scripts\remote_compute.ps1 -Action publish-data
   .\scripts\remote_compute.ps1 -Action evolve-limit-up
   ```

3. 看结果中的三部分：结构公式、训练/验证/锁定测试、未准入原因。只有结论显示
   `可操作` 或状态显示 `已通过`，才进入下一步。

4. 生成实际观察/操作卡：

   本地挖掘后：

   ```powershell
   uv run python -m cli zt status
   uv run python -m cli zt scan
   ```

   服务器挖掘后，状态文件留在服务器映射盘，需要明确指定：

   ```powershell
   uv run python -m cli zt status --state X:\alpha-miner\data\limit_up_evolution.json
   uv run python -m cli zt scan --state X:\alpha-miner\data\limit_up_evolution.json
   ```

当前只有 16 个可用信号日，低于 40 日门槛。现在可以运行挖掘观察系统行为，但继续增加代数
不能替代新数据，输出仍应是 `WATCH_ONLY`。

通用 IC 因子挖掘属于开发研究入口，不建议作为日常股票操作流程。确需运行时使用：

```powershell
uv run python -m cli mine evolve --generations 5 --population 12 --workers 4
```

## 怎么读操作卡

| 字段 | 您要做什么 |
|------|------------|
| CONDITIONAL_BUY | 只有因子已通过锁定测试，并且次日开盘仍满足入场条件时才考虑 |
| WATCH_ONLY | 只加入观察，不买；常见原因是样本或测试未达标 |
| AVOID | 风险结构触发否决，直接回避 |
| 结构分 | 当日涨停池内部的相对排序，不是上涨概率，也不是目标收益 |
| 入场 | T1 开盘的条件；一字涨停、开盘涨幅超阈值或无法成交就放弃 |
| 退出 | 买入后第 1/2 个完整交易日收盘退出，遵守 A 股 T+1 |
| 仓位 | 单票与总持仓限制；因子未准入时始终为 0 |
| 主要贡献 | 说明这只股票为何排在前面，以及风险项如何扣分 |

不要把“候选排名第一”理解成“明天最可能涨”。它只代表这只股票最符合当前结构公式。

## 因子到底如何告诉我操作

以涨停股为例：

1. T0 收盘后，因子观察封板质量、连板结构、板块扩散、资金确认和开板风险；
2. 多个因子合成一个可读结构，只在当日涨停池内排序；
3. T1 开盘再检查是否能买、是否高开过多，条件不满足就放弃；
4. 卡片预先写明退出日和最大仓位，不能盘中临时改成无限持有；
5. 锁定测试不合格时，即使分数很高也只能 `WATCH_ONLY`。

这条链路的重点不是每天都给买点，而是在证据不足时明确告诉您“不操作”。

## 分析普通股票走势

涨停结构因子只适用于已经进入 T0 涨停池的股票，不能解释所有普通股票。普通股票可查看市场环境、候选卡和持仓风险：

```powershell
uv run python -m cli report --brief
uv run python -m cli report --brief --holdings 600000,000001
```

当前通用因子也尚未产生通过完整准入的新因子，因此这部分只作为市场背景和风险辅助，不作为自动买卖指令。

## 三条硬规则

- 数据日期不是最新交易日：不使用；
- 操作卡不是 `CONDITIONAL_BUY`：不买；
- 次日实际开盘不满足卡片条件：放弃，不追价。

系统不会连接券商自动下单。任何实盘决定与风险仍由使用者承担。
