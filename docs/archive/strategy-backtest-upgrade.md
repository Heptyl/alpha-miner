# Alpha Miner v2 — 策略回测系统 + 质量自检

> **设计**: Opus (架构)
> **执行**: GLM (代码)
> **核心转变**: 从"因子打分系统"转为"策略买卖回测系统"
> **质量保证**: 每步内置自检脚本，不通过则回退重做

---

## 零、为什么要做

当前系统的问题: IC=0.05, ICIR=0.6, 综合分 7.2 — 这些数字你看了以后还是不知道该不该买、买了以后什么时候卖。

目标: 系统输出必须长这样:

```
策略: 题材启动期龙头首板
回测 120 天，共 47 笔交易
胜率 63.8% | 平均赚 4.2% | 平均亏 -2.8% | 盈亏比 1.5
最大连亏 4 笔 | 最大回撤 -8.3% | 总收益 +38.7%

今日符合条件的股票:
  000001 (AI题材, 首板, 龙头清晰度0.8) → 明日集合竞价高开<3%可介入
  000035 (机器人, 2板, 题材发酵期) → 明日集合竞价高开<5%可介入

持仓 600519:
  入场条件已不满足(题材进入衰退期) → 建议明日开盘卖出
```

---

## 一、数据结构

### 1.1 策略定义 (`Strategy`)

```python
# src/strategy/schema.py

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class EntryRule:
    """入场规则: 满足全部条件才触发买入信号。"""
    regime_filter: list[str] = field(default_factory=list)
    # 例: ["board_rally", "theme_rotation"]
    # 空列表 = 不限 regime

    conditions: list[dict] = field(default_factory=list)
    # 例: [
    #   {"factor": "theme_lifecycle", "op": ">=", "value": 0.5},
    #   {"factor": "consecutive_board", "op": ">=", "value": 1},
    #   {"factor": "leader_clarity", "op": ">=", "value": 0.4},
    # ]

    timing: str = "next_open"
    # "next_open" = 次日开盘价买入
    # "next_open_if_gap_lt_N" = 次日高开 < N% 才买


@dataclass
class ExitRule:
    """出场规则: 任一条件触发即卖出。"""
    take_profit_pct: float = 5.0       # 盈利 N% 止盈
    stop_loss_pct: float = -3.0        # 亏损 N% 止损
    max_hold_days: int = 3             # 最大持有天数
    trailing_stop_pct: Optional[float] = None  # 移动止损(可选)

    exit_conditions: list[dict] = field(default_factory=list)
    # 条件出场，例:
    # [
    #   {"factor": "theme_lifecycle", "op": "<", "value": 0.3,
    #    "reason": "题材进入衰退期"},
    #   {"factor": "fund_flow_diverge", "op": ">", "value": 0.5,
    #    "reason": "大小单背离，疑似出货"},
    # ]


@dataclass
class PositionRule:
    """仓位规则。"""
    single_position_pct: float = 20.0  # 单票仓位上限(%)
    max_holdings: int = 3              # 同时最多持有
    total_position_pct: float = 80.0   # 总仓位上限(%)


@dataclass
class Strategy:
    """一个完整的可回测策略。"""
    name: str
    description: str
    entry: EntryRule
    exit: ExitRule
    position: PositionRule = field(default_factory=PositionRule)
    tags: list[str] = field(default_factory=list)  # ["打板", "龙头", "首板"]

    # 元信息
    version: int = 1
    source: str = "manual"     # "manual" / "evolved" / "knowledge_base"
    parent: Optional[str] = None  # 进化来源
    created_at: str = ""
```

### 1.2 交易记录 (`Trade`)

```python
@dataclass
class Trade:
    """一笔完整的交易记录(回测产生)。"""
    strategy_name: str
    stock_code: str
    stock_name: str = ""

    # 入场
    entry_date: str = ""          # 买入日期
    entry_price: float = 0.0      # 买入价格
    entry_reason: str = ""        # 触发的入场条件描述

    # 出场
    exit_date: str = ""           # 卖出日期
    exit_price: float = 0.0       # 卖出价格
    exit_reason: str = ""         # "take_profit" / "stop_loss" / "max_hold" / "condition:xxx"

    # 结果
    return_pct: float = 0.0       # 收益率(%)
    hold_days: int = 0            # 持有天数
    max_drawdown_pct: float = 0.0 # 持仓期间最大回撤(%)

    # 当时的市场环境(便于复盘)
    regime_at_entry: str = ""
    emotion_at_entry: str = ""
```

### 1.3 策略回测报告 (`StrategyReport`)

```python
@dataclass
class StrategyReport:
    """策略回测结果。"""
    strategy_name: str
    backtest_start: str
    backtest_end: str
    total_trades: int = 0
    win_count: int = 0
    loss_count: int = 0
    win_rate: float = 0.0          # 胜率
    avg_win_pct: float = 0.0       # 平均盈利(%)
    avg_loss_pct: float = 0.0      # 平均亏损(%)
    profit_loss_ratio: float = 0.0 # 盈亏比
    max_consecutive_loss: int = 0  # 最大连亏
    max_drawdown_pct: float = 0.0  # 最大回撤(%)
    total_return_pct: float = 0.0  # 总收益(%)
    sharpe_ratio: float = 0.0      # 夏普比率
    trades: list[Trade] = field(default_factory=list)

    # 分 regime 统计
    regime_stats: dict = field(default_factory=dict)
    # 格式: {"board_rally": {"trades": 15, "win_rate": 0.73, ...}, ...}
```

---

## 二、策略回测引擎 (`src/strategy/backtest_engine.py`)

这是核心。它不算 IC，而是**模拟真实的逐笔交易**。

```python
# src/strategy/backtest_engine.py

class BacktestEngine:
    """逐笔交易模拟引擎。

    关键设计:
    1. T+1 限制: 今天买的明天才能卖
    2. 涨跌停限制: 涨停无法买入，跌停无法卖出
    3. 时间隔离: 只用 as_of 之前的数据判断
    4. 仓位约束: 遵守 PositionRule
    """

    def __init__(self, db: Storage):
        self.db = db
        self.registry = FactorRegistry()
        self.regime_detector = RegimeDetector(db)

    def run(
        self,
        strategy: Strategy,
        start_date: str,
        end_date: str,
        universe_source: str = "zt_pool",  # 从哪里选股票池
        initial_capital: float = 1_000_000,
    ) -> StrategyReport:
        """运行策略回测。"""

        trade_dates = self._get_trade_dates(start_date, end_date)
        holdings = {}      # {stock_code: HoldingInfo}
        all_trades = []     # 已完成的交易列表
        capital = initial_capital
        peak_capital = initial_capital

        for i, date in enumerate(trade_dates):
            as_of = datetime.strptime(date, "%Y-%m-%d").replace(hour=15)

            # ── Step 1: 检查出场 ──
            # 对所有持仓，检查是否触发出场条件
            codes_to_exit = []
            for code, holding in holdings.items():
                exit_reason = self._check_exit(
                    strategy.exit, code, date, holding, as_of
                )
                if exit_reason:
                    codes_to_exit.append((code, exit_reason))

            for code, reason in codes_to_exit:
                trade = self._execute_exit(
                    holdings.pop(code), date, reason, as_of
                )
                all_trades.append(trade)
                capital += trade.exit_price  # 简化: 用收益率计算

            # ── Step 2: 检查入场 ──
            # 只在仓位未满时寻找新机会
            if len(holdings) >= strategy.position.max_holdings:
                continue

            # 获取今日候选池
            universe = self._get_universe(date, universe_source, as_of)
            if not universe:
                continue

            # 检查 regime 过滤
            regime_info = self.regime_detector.detect(as_of)
            if strategy.entry.regime_filter:
                if regime_info.regime not in strategy.entry.regime_filter:
                    continue

            # 对候选池逐只检查入场条件
            for code in universe:
                if code in holdings:
                    continue
                if len(holdings) >= strategy.position.max_holdings:
                    break

                entry_match = self._check_entry(
                    strategy.entry, code, date, as_of
                )
                if entry_match:
                    holding = self._execute_entry(
                        code, date, strategy, regime_info, as_of
                    )
                    if holding:
                        holdings[code] = holding

            # 更新净值用于计算回撤
            current_value = self._calc_portfolio_value(
                capital, holdings, date, as_of
            )
            peak_capital = max(peak_capital, current_value)

        # 结束: 强制平仓
        last_date = trade_dates[-1] if trade_dates else end_date
        for code, holding in holdings.items():
            trade = self._execute_exit(
                holding, last_date, "backtest_end",
                datetime.strptime(last_date, "%Y-%m-%d").replace(hour=15)
            )
            all_trades.append(trade)

        return self._build_report(strategy.name, start_date, end_date, all_trades)

    # ── 入场检查 ─────────────────────────────────

    def _check_entry(self, entry: EntryRule, code: str, date: str,
                     as_of: datetime) -> bool:
        """检查一只股票是否满足所有入场条件。"""
        for cond in entry.conditions:
            factor_name = cond["factor"]
            op = cond["op"]
            threshold = cond["value"]

            # 获取该因子在该股票上的值
            factor_value = self._get_factor_value(factor_name, code, date, as_of)
            if factor_value is None:
                return False

            if not self._compare(factor_value, op, threshold):
                return False

        return True

    # ── 出场检查 ─────────────────────────────────

    def _check_exit(self, exit_rule: ExitRule, code: str, date: str,
                    holding, as_of: datetime) -> str | None:
        """检查是否触发出场。返回出场原因或 None。"""
        current_price = self._get_price(code, date, as_of)
        if current_price is None:
            return None

        return_pct = (current_price - holding.entry_price) / holding.entry_price * 100
        hold_days = self._count_trade_days(holding.entry_date, date)

        # 止盈
        if return_pct >= exit_rule.take_profit_pct:
            return f"take_profit:{return_pct:.1f}%"

        # 止损
        if return_pct <= exit_rule.stop_loss_pct:
            return f"stop_loss:{return_pct:.1f}%"

        # 时间止损
        if hold_days >= exit_rule.max_hold_days:
            return f"max_hold:{hold_days}d"

        # 条件出场
        for cond in exit_rule.exit_conditions:
            factor_value = self._get_factor_value(
                cond["factor"], code, date, as_of
            )
            if factor_value is not None:
                if self._compare(factor_value, cond["op"], cond["value"]):
                    return f"condition:{cond.get('reason', cond['factor'])}"

        return None

    # ── 辅助方法 ─────────────────────────────────

    def _get_factor_value(self, factor_name: str, code: str,
                          date: str, as_of: datetime) -> float | None:
        """获取某只股票在某日的因子值。"""
        df = self.db.query(
            "factor_values", as_of,
            where="factor_name = ? AND stock_code = ? AND trade_date = ?",
            params=(factor_name, code, date),
        )
        if df.empty:
            return None
        return float(df.iloc[-1]["factor_value"])

    def _get_price(self, code: str, date: str,
                   as_of: datetime) -> float | None:
        """获取某日收盘价。"""
        df = self.db.query(
            "daily_price", as_of,
            where="stock_code = ? AND trade_date = ?",
            params=(code, date),
        )
        if df.empty:
            return None
        return float(df.iloc[-1]["close"])

    def _get_universe(self, date: str, source: str,
                      as_of: datetime) -> list[str]:
        """获取候选股票池。"""
        df = self.db.query(
            source, as_of,
            where="trade_date = ?", params=(date,),
        )
        if df.empty:
            return []
        return df["stock_code"].unique().tolist()

    def _compare(self, value: float, op: str, threshold: float) -> bool:
        """比较运算。"""
        ops = {
            ">=": lambda a, b: a >= b,
            "<=": lambda a, b: a <= b,
            ">": lambda a, b: a > b,
            "<": lambda a, b: a < b,
            "==": lambda a, b: abs(a - b) < 1e-6,
        }
        return ops.get(op, lambda a, b: False)(value, threshold)

    def _get_trade_dates(self, start: str, end: str) -> list[str]:
        """获取区间内的交易日列表。"""
        df = self.db.query_range(
            "daily_price",
            datetime.strptime(end, "%Y-%m-%d"),
            lookback_days=(datetime.strptime(end, "%Y-%m-%d") -
                           datetime.strptime(start, "%Y-%m-%d")).days + 10,
        )
        if df.empty:
            return []
        dates = sorted(df["trade_date"].unique())
        return [d for d in dates if start <= d <= end]

    def _count_trade_days(self, d1: str, d2: str) -> int:
        """计算两个日期之间的交易日数。"""
        all_dates = self._get_trade_dates(d1, d2)
        return len(all_dates)

    def _build_report(self, name, start, end, trades) -> StrategyReport:
        """从交易列表构建回测报告。"""
        report = StrategyReport(
            strategy_name=name,
            backtest_start=start,
            backtest_end=end,
            total_trades=len(trades),
            trades=trades,
        )
        if not trades:
            return report

        returns = [t.return_pct for t in trades]
        wins = [r for r in returns if r > 0]
        losses = [r for r in returns if r <= 0]

        report.win_count = len(wins)
        report.loss_count = len(losses)
        report.win_rate = len(wins) / len(returns) if returns else 0
        report.avg_win_pct = sum(wins) / len(wins) if wins else 0
        report.avg_loss_pct = sum(losses) / len(losses) if losses else 0
        report.profit_loss_ratio = (
            abs(report.avg_win_pct / report.avg_loss_pct)
            if report.avg_loss_pct != 0 else float('inf')
        )

        # 最大连亏
        max_consec = 0
        cur_consec = 0
        for r in returns:
            if r <= 0:
                cur_consec += 1
                max_consec = max(max_consec, cur_consec)
            else:
                cur_consec = 0
        report.max_consecutive_loss = max_consec

        # 总收益(简单累加)
        report.total_return_pct = sum(returns)

        # 按 regime 分组统计
        regime_groups = {}
        for t in trades:
            r = t.regime_at_entry or "unknown"
            if r not in regime_groups:
                regime_groups[r] = []
            regime_groups[r].append(t.return_pct)

        for r, rets in regime_groups.items():
            w = [x for x in rets if x > 0]
            report.regime_stats[r] = {
                "trades": len(rets),
                "win_rate": len(w) / len(rets) if rets else 0,
                "avg_return": sum(rets) / len(rets) if rets else 0,
            }

        return report
```

---

## 三、预置策略库 (`knowledge_base/strategies.yaml`)

从 `theories.yaml` 的 12 个假说转化为可回测的策略:

```yaml
# knowledge_base/strategies.yaml

strategies:
  - name: "首板打板_龙头确认"
    description: "题材启动期，龙头首板次日低开介入"
    tags: ["打板", "龙头", "首板"]
    source: "info_cascade + theme_lifecycle"
    entry:
      regime_filter: ["board_rally", "theme_rotation", "normal"]
      conditions:
        - {factor: "consecutive_board", op: ">=", value: 1}
        - {factor: "theme_lifecycle", op: ">=", value: 0.5}
        - {factor: "leader_clarity", op: ">=", value: 0.4}
      timing: "next_open"
    exit:
      take_profit_pct: 7.0
      stop_loss_pct: -3.0
      max_hold_days: 3
      exit_conditions:
        - {factor: "theme_lifecycle", op: "<", value: 0.2,
           reason: "题材衰退"}
    position:
      single_position_pct: 25.0
      max_holdings: 3

  - name: "题材发酵_跟风低吸"
    description: "题材爆发期，非龙头低吸抢反包"
    tags: ["低吸", "跟风", "反包"]
    source: "theme_lifecycle"
    entry:
      regime_filter: ["theme_rotation", "normal"]
      conditions:
        - {factor: "theme_lifecycle", op: ">=", value: 0.6}
        - {factor: "leader_clarity", op: "<", value: 0.4}
        - {factor: "narrative_velocity", op: ">", value: 0}
        - {factor: "turnover_rank", op: ">=", value: 0.3}
      timing: "next_open"
    exit:
      take_profit_pct: 5.0
      stop_loss_pct: -4.0
      max_hold_days: 2
    position:
      single_position_pct: 15.0
      max_holdings: 4

  - name: "情绪冰点_反弹首板"
    description: "极弱情绪持续3日后首个涨停股"
    tags: ["反转", "冰点", "首板"]
    source: "emotion_regime"
    entry:
      regime_filter: []  # 不限，但条件里有情绪要求
      conditions:
        - {factor: "zt_dt_ratio", op: "<", value: 0.3}
        - {factor: "consecutive_board", op: "==", value: 1}
      timing: "next_open"
    exit:
      take_profit_pct: 10.0
      stop_loss_pct: -5.0
      max_hold_days: 5
    position:
      single_position_pct: 20.0
      max_holdings: 2

  - name: "三班组回避"
    description: "识别游资对倒出货，生成卖出信号"
    tags: ["风控", "出货", "回避"]
    source: "three_shift"
    entry:
      # 这是一个反向策略: 满足条件的股票应该卖出/回避
      regime_filter: []
      conditions:
        - {factor: "consecutive_board", op: ">=", value: 3}
        - {factor: "turnover_rank", op: "<", value: 0.2}
        - {factor: "leader_clarity", op: "<", value: 0.3}
        - {factor: "main_flow_intensity", op: "<", value: -0.3}
      timing: "next_open"
    exit:
      take_profit_pct: 3.0
      stop_loss_pct: -2.0
      max_hold_days: 1
    position:
      single_position_pct: 10.0
      max_holdings: 1
```

---

## 四、策略进化引擎 (`src/strategy/evolver.py`)

进化的单位从"因子"变成"策略":

```python
# src/strategy/evolver.py

class StrategyEvolver:
    """策略进化器: 调整入场/出场条件，保留胜率高的版本。"""

    MUTATIONS = [
        "adjust_entry_threshold",   # 入场阈值 ±10%/±20%
        "add_entry_condition",      # 从因子库增加一个入场条件
        "remove_entry_condition",   # 去掉一个入场条件
        "adjust_exit_params",       # 止盈/止损/持仓天数调整
        "add_exit_condition",       # 增加条件出场
        "change_regime_filter",     # 换适用的 regime
        "adjust_position_size",     # 调整仓位
    ]

    def __init__(self, db: Storage, engine: BacktestEngine):
        self.db = db
        self.engine = engine

    def evolve(
        self,
        base_strategy: Strategy,
        start_date: str,
        end_date: str,
        generations: int = 10,
        mutations_per_gen: int = 5,
    ) -> list[tuple[Strategy, StrategyReport]]:
        """对一个基础策略做进化。

        每代:
        1. 对当前最优策略生成 N 个变异版本
        2. 回测所有变异
        3. 保留胜率×盈亏比最高的版本
        4. 下一代基于最优版本继续变异

        返回: 所有代的最优策略和报告
        """
        best = base_strategy
        best_report = self.engine.run(best, start_date, end_date)
        best_score = self._score(best_report)

        history = [(best, best_report)]

        for gen in range(generations):
            candidates = []
            for _ in range(mutations_per_gen):
                mutated = self._mutate(best)
                report = self.engine.run(mutated, start_date, end_date)
                score = self._score(report)
                candidates.append((mutated, report, score))

            # 选最优
            candidates.sort(key=lambda x: x[2], reverse=True)
            if candidates and candidates[0][2] > best_score:
                best, best_report, best_score = candidates[0]
                history.append((best, best_report))

        return history

    def _score(self, report: StrategyReport) -> float:
        """策略评分 = 胜率 × 盈亏比 × log(交易次数)。
        交易次数太少的策略打折扣。
        """
        if report.total_trades < 5:
            return 0.0
        import math
        return (
            report.win_rate
            * report.profit_loss_ratio
            * math.log(report.total_trades + 1)
        )

    def _mutate(self, strategy: Strategy) -> Strategy:
        """随机选择一种变异方式。"""
        import random
        from copy import deepcopy

        s = deepcopy(strategy)
        s.version += 1
        s.parent = strategy.name
        s.source = "evolved"

        mutation = random.choice(self.MUTATIONS)

        if mutation == "adjust_entry_threshold":
            if s.entry.conditions:
                idx = random.randint(0, len(s.entry.conditions) - 1)
                cond = s.entry.conditions[idx]
                delta = random.choice([0.9, 0.8, 1.1, 1.2])
                cond["value"] = round(cond["value"] * delta, 2)

        elif mutation == "adjust_exit_params":
            param = random.choice(["take_profit_pct", "stop_loss_pct", "max_hold_days"])
            if param == "take_profit_pct":
                s.exit.take_profit_pct += random.choice([-1, -2, 1, 2])
            elif param == "stop_loss_pct":
                s.exit.stop_loss_pct += random.choice([-1, 1])
            elif param == "max_hold_days":
                s.exit.max_hold_days += random.choice([-1, 1])
                s.exit.max_hold_days = max(1, s.exit.max_hold_days)

        elif mutation == "remove_entry_condition":
            if len(s.entry.conditions) > 1:
                idx = random.randint(0, len(s.entry.conditions) - 1)
                s.entry.conditions.pop(idx)

        elif mutation == "change_regime_filter":
            all_regimes = ["board_rally", "theme_rotation", "normal", "broad_move"]
            s.entry.regime_filter = random.sample(all_regimes, random.randint(1, 3))

        # 其他变异方式类似...
        s.name = f"{strategy.name}_v{s.version}"
        return s
```

---

## 五、CLI 集成

### 5.1 新命令

```python
# cli/strategy.py

@click.group()
def strategy():
    """策略管理与回测。"""
    pass


@strategy.command("backtest")
@click.option("--name", required=True, help="策略名称(strategies.yaml 中定义)")
@click.option("--start", required=True, help="开始日期 YYYY-MM-DD")
@click.option("--end", required=True, help="结束日期 YYYY-MM-DD")
def cmd_backtest(name, start, end):
    """回测一个策略，输出逐笔交易明细。"""
    # 1. 从 strategies.yaml 加载策略
    # 2. 实例化 BacktestEngine
    # 3. 运行 engine.run(strategy, start, end)
    # 4. rich 格式化输出 StrategyReport
    # 5. 逐笔交易明细(每笔一行: 日期/股票/方向/收益/原因)
    pass


@strategy.command("evolve")
@click.option("--name", required=True, help="基础策略名称")
@click.option("--start", required=True)
@click.option("--end", required=True)
@click.option("--generations", default=10)
def cmd_evolve(name, start, end, generations):
    """对策略做进化优化。"""
    pass


@strategy.command("scan")
@click.option("--date", default=None, help="扫描日期")
def cmd_scan(date):
    """扫描今日所有策略，输出买入/卖出信号。"""
    # 1. 加载所有策略
    # 2. 对每个策略，检查今日 universe 中哪些股票满足入场条件
    # 3. 对当前持仓，检查哪些股票触发出场条件
    # 4. 输出: 买入信号列表 + 卖出信号列表
    pass


@strategy.command("list")
def cmd_list():
    """列出所有策略及其最近回测成绩。"""
    pass
```

### 5.2 更新 daily_run.sh

```bash
# 在现有流程末尾追加:
python -m cli strategy scan          # 今日买卖信号
python -m cli strategy backtest --name "首板打板_龙头确认" --start $START --end $DATE  # 滚动回测
```

---

## 六、数据库变更

```sql
-- 策略回测结果存储
CREATE TABLE IF NOT EXISTS strategy_reports (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name TEXT NOT NULL,
    backtest_start TEXT,
    backtest_end TEXT,
    total_trades INTEGER DEFAULT 0,
    win_rate REAL DEFAULT 0.0,
    avg_win_pct REAL DEFAULT 0.0,
    avg_loss_pct REAL DEFAULT 0.0,
    profit_loss_ratio REAL DEFAULT 0.0,
    total_return_pct REAL DEFAULT 0.0,
    max_drawdown_pct REAL DEFAULT 0.0,
    report_json TEXT,          -- 完整报告 JSON
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(strategy_name, backtest_start, backtest_end)
);

-- 交易明细
CREATE TABLE IF NOT EXISTS strategy_trades (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_name TEXT NOT NULL,
    stock_code TEXT NOT NULL,
    entry_date TEXT,
    entry_price REAL,
    exit_date TEXT,
    exit_price REAL,
    return_pct REAL,
    exit_reason TEXT,
    regime_at_entry TEXT,
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 今日信号(每日扫描结果)
CREATE TABLE IF NOT EXISTS strategy_signals (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    strategy_name TEXT NOT NULL,
    signal_type TEXT NOT NULL,  -- "entry" / "exit"
    stock_code TEXT NOT NULL,
    reason TEXT,
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date, strategy_name, signal_type, stock_code)
);
```

---

## 七、质量自检机制 (GLM 必须执行)

**每完成一个 Step，GLM 必须运行对应的自检脚本。不通过则回退重做，不得跳过。**

### 自检原则

1. 每个 Step 末尾有一个 `验收命令`，GLM 必须运行并贴出完整输出
2. 如果验收命令报错或断言失败，GLM 必须修复后重新运行
3. 连续 3 次未通过同一验收，GLM 必须输出完整错误信息并暂停等待人工介入
4. 最终验收(Step 8)是一个端到端集成测试，模拟完整的回测→信号流程

### Step 验收表

```
Step 1 (数据结构):
  验收: pytest tests/test_strategy_schema.py -v
  断言: Strategy/Trade/StrategyReport 能正确序列化/反序列化 YAML

Step 2 (回测引擎):
  验收: pytest tests/test_backtest_engine.py -v
  断言:
    - test_entry_check: 给定因子值，正确判断是否满足入场条件
    - test_exit_check: 止盈/止损/时间止损/条件出场 各触发一次
    - test_t1_constraint: T+1 限制生效
    - test_empty_universe: 空候选池不报错
    - test_position_limit: 不超过 max_holdings
    测试数量 >= 8

Step 3 (预置策略):
  验收: python -c "
import yaml
strategies = yaml.safe_load(open('knowledge_base/strategies.yaml'))['strategies']
assert len(strategies) >= 4, f'策略数 {len(strategies)} < 4'
for s in strategies:
    assert 'entry' in s, f'{s[\"name\"]} 缺少 entry'
    assert 'exit' in s, f'{s[\"name\"]} 缺少 exit'
    assert len(s['entry']['conditions']) >= 2, f'{s[\"name\"]} 入场条件不足'
print(f'✅ {len(strategies)} 个策略验证通过')
"

Step 4 (策略进化):
  验收: pytest tests/test_strategy_evolver.py -v
  断言:
    - test_mutation_changes_strategy: 变异后策略与原策略不同
    - test_score_prefers_high_winrate: 高胜率策略得分更高
    - test_evolution_improves: 10代后最优策略得分 >= 初代

Step 5 (CLI):
  验收:
    python -m cli strategy list
    python -m cli strategy backtest --name "首板打板_龙头确认" --start 2024-01-01 --end 2024-06-30
  断言: 两个命令均不报错，backtest 输出包含 "胜率" 和 "盈亏比"

Step 6 (数据库):
  验收: python -c "
from src.data.storage import Storage
db = Storage()
db.init_db()
# 检查新表存在
import sqlite3
conn = sqlite3.connect(db.db_path)
tables = [r[0] for r in conn.execute(\"SELECT name FROM sqlite_master WHERE type='table'\").fetchall()]
for t in ['strategy_reports', 'strategy_trades', 'strategy_signals']:
    assert t in tables, f'表 {t} 不存在'
print('✅ 数据库表验证通过')
"

Step 7 (整合现有因子):
  验收: pytest tests/ -v -m "not live" --tb=short
  断言: 全部现有测试 + 全部新测试通过，总数 >= 80

Step 8 (端到端):
  验收: python -c "
# 端到端集成测试
from datetime import datetime
from src.data.storage import Storage
from src.strategy.backtest_engine import BacktestEngine
from src.strategy.schema import Strategy, EntryRule, ExitRule

db = Storage('data/alpha_miner.db')
engine = BacktestEngine(db)

# 用最简单的策略做冒烟测试
s = Strategy(
    name='smoke_test',
    description='冒烟测试',
    entry=EntryRule(conditions=[
        {'factor': 'consecutive_board', 'op': '>=', 'value': 1}
    ]),
    exit=ExitRule(take_profit_pct=5, stop_loss_pct=-3, max_hold_days=3),
)
# 如果有数据则回测，无数据则跳过
try:
    report = engine.run(s, '2024-01-01', '2024-06-30')
    print(f'✅ 端到端通过: {report.total_trades} 笔交易')
    if report.total_trades > 0:
        print(f'   胜率: {report.win_rate:.1%}')
        print(f'   盈亏比: {report.profit_loss_ratio:.2f}')
except Exception as e:
    if 'no data' in str(e).lower() or report.total_trades == 0:
        print('⚠️ 无数据，逻辑通过但需回填数据后重测')
    else:
        raise
"
```

---

## 八、执行步骤

### Step 1: 数据结构
- [ ] 创建 `src/strategy/__init__.py`
- [ ] 创建 `src/strategy/schema.py` (Strategy/Trade/StrategyReport/EntryRule/ExitRule/PositionRule)
- [ ] 写测试 `tests/test_strategy_schema.py` — 序列化/反序列化验证
- [ ] **运行验收**: `pytest tests/test_strategy_schema.py -v`

### Step 2: 回测引擎 (核心)
- [ ] 创建 `src/strategy/backtest_engine.py`
- [ ] 实现全部方法: `run`, `_check_entry`, `_check_exit`, `_execute_entry`, `_execute_exit`, `_build_report`
- [ ] 写测试 `tests/test_backtest_engine.py` — 至少 8 个测试用例，用 mock 数据
- [ ] **运行验收**: `pytest tests/test_backtest_engine.py -v`
- [ ] **特别注意**: T+1 限制必须生效，涨跌停限制必须生效

### Step 3: 预置策略库
- [ ] 创建 `knowledge_base/strategies.yaml` (至少 4 个策略)
- [ ] 写加载器: `src/strategy/loader.py` — 从 YAML 加载为 Strategy 对象
- [ ] **运行验收**: 贴在 Step 验收表里的 python -c 命令

### Step 4: 策略进化器
- [ ] 创建 `src/strategy/evolver.py`
- [ ] 实现全部 7 种变异方式
- [ ] 写测试 `tests/test_strategy_evolver.py` — 至少 5 个测试
- [ ] **运行验收**: `pytest tests/test_strategy_evolver.py -v`

### Step 5: CLI 集成
- [ ] 创建 `cli/strategy.py` — backtest/evolve/scan/list 四个子命令
- [ ] 注册到 `cli/__main__.py`
- [ ] 用 rich 格式化回测报告输出(表格 + 逐笔明细)
- [ ] **运行验收**: 运行 list 和 backtest 命令，贴出完整输出

### Step 6: 数据库变更
- [ ] 在 `src/data/schema.sql` 追加三张新表
- [ ] 在 `backtest_engine.py` 的 `run()` 末尾自动存入 strategy_reports 和 strategy_trades
- [ ] **运行验收**: 贴在 Step 验收表里的 python -c 命令

### Step 7: 与现有系统整合
- [ ] 在 `DailyBrief.build_candidates()` 中: 不再用 IC 加权打分，改为调用策略扫描
- [ ] 保持旧接口不变(仍输出 CandidateCard)，但卡片内容来自策略信号
- [ ] 更新 `scripts/daily_run.sh` 加入策略扫描步骤
- [ ] **运行验收**: `pytest tests/ -v -m "not live" --tb=short`

### Step 8: 端到端验证
- [ ] 运行验收表中的端到端 python -c 脚本
- [ ] 更新 README.md, CLAUDE.md, BUILD_LOG.md
- [ ] **最终验收**: 全量测试通过，端到端冒烟测试通过

---

## 九、关键约束

1. **不删现有代码**: 回测引擎是新增模块，现有的 IC Tracker / DailyBrief / EvolutionEngine 保留不动。Step 7 是在 DailyBrief 内部替换数据来源，接口不变。
2. **时间隔离**: `BacktestEngine` 的所有数据查询必须通过 `db.query(table, as_of=...)`。
3. **T+1 + 涨跌停**: 回测必须模拟 A 股规则。今日信号次日执行，涨停买不进，跌停卖不出。
4. **自检不可跳过**: 每个 Step 的验收命令必须运行。如果 GLM 说"验收通过"但没贴出命令输出，视为未通过。
5. **测试数量**: 新增测试 >= 25 个，总测试数 >= 90 个。

---

## 十、给人类的检查清单

当 GLM 完成后，你只需要检查以下几件事:

```bash
# 1. 全量测试是否通过
pytest tests/ -v -m "not live" | tail -5
# 期望: "XX passed" 且 XX >= 90

# 2. 随便跑一个策略回测看输出是否合理
python -m cli strategy backtest --name "首板打板_龙头确认" --start 2024-01-01 --end 2024-06-30
# 期望: 看到逐笔交易明细，胜率/盈亏比/总收益等数字

# 3. 策略扫描是否输出今日信号
python -m cli strategy scan
# 期望: 看到 "买入信号" 和 "卖出信号" 列表(可能为空，但不报错)

# 4. 进化是否能跑通
python -m cli strategy evolve --name "首板打板_龙头确认" --start 2024-01-01 --end 2024-06-30 --generations 3
# 期望: 看到每代最优策略的胜率变化
```

如果以上 4 个命令都正常，系统就可用了。剩下的是积累数据和持续进化。
