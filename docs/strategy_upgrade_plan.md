# Alpha Miner 选股策略升级计划 — 9维选股体系

> 目标: 实盘操作盈利概率 ≥ 90%
> 日期: 2026-05-08
> 状态: 规划中

## 核心理念

90%胜率 = 多层过滤漏斗。每一层过滤掉不合格的标的，最终留下的都是经过：
- 技术面确认（趋势/量价）
- 基本面确认（盈利/估值）
- 资金面确认（主力/北向）
- 风控确认（无雷/无险）
的标的。

## 当前能力 vs 需要

| 维度 | 已有 | 缺失 | 数据源 |
|------|------|------|--------|
| 1.趋势突破 | 无独立策略 | ❌ 需新建 | daily_price (已有) |
| 2.缩量回调 | 无 | ❌ 需新建 | daily_price (已有) |
| 3.资金连续流入 | fund_flow表 | ❌ 需建策略 | fund_flow (已有) |
| 4.板块轮动 | concept_mapping | ❌ 需建策略 | zt_pool+concept (已有) |
| 5.量价筛选 | daily_price | ❌ 无PE数据 | akshare需采集 |
| 6.基本面排雷 | 无 | ❌ 完全缺失 | akshare需采集 |
| 7.北向资金 | 无 | ❌ 完全缺失 | akshare需采集 |
| 8.行业景气 | 无 | ❌ 完全缺失 | akshare需采集 |
| 9.风控 | 无 | ❌ 完全缺失 | akshare需采集 |

## 分层架构

```
Layer 1: 全市场扫描 (2700+只)
  → 趋势突破 + 缩量回调 + 量价筛选
  → 输出: ~200只候选

Layer 2: 基本面过滤
  → 3年净利润/ROE/PE/排除ST退市风险
  → 输出: ~80只合格

Layer 3: 资金面确认
  → 主力连续流入 + 北向加仓
  → 输出: ~30只确认

Layer 4: 行业景气 + 政策利好
  → 高增长赛道优先
  → 输出: ~15只

Layer 5: 风控终审
  → 回撤/解禁/估值分位/质押率
  → 输出: 5-10只最终推荐

Layer 6: 买入时机 (次日盘中)
  → 竞价观察 → 低吸区间 → 止损止盈
```

## 新增数据采集

### Phase A: 数据基础设施 (优先)

1. **stock_fundamentals 表** — 基本面指标
   - 采集: ak.stock_financial_analysis_indicator()
   - 字段: stock_code, report_date, roe, roa, net_profit_margin, gross_margin, eps, revenue_yoy, profit_yoy, pe, pb, debt_ratio
   - 频率: 季度更新
   - 覆盖: 全A股

2. **northbound_flow 表** — 北向资金
   - 采集: ak.stock_hsgt_hold_stock_em() + ak.stock_hsgt_individual_em()
   - 字段: stock_code, trade_date, hold_amount, hold_change_pct, buy_amount, sell_amount
   - 频率: 每日
   - 覆盖: 北向持股标的

3. **stock_valuation 表** — 估值数据
   - 采集: ak.stock_a_indicator_lg() 或 ak.stock_financial_analysis_indicator_em()
   - 字段: stock_code, trade_date, pe_ttm, pb, ps_ttm, dv_ratio
   - 频率: 每日

4. **restricted_shares 表** — 解禁数据
   - 采集: ak.stock_restricted_release_queue_em()
   - 字段: stock_code, unlock_date, unlock_amount, unlock_ratio, unlock_type
   - 频率: 每周

5. **industry_pe 表** — 行业PE/景气度
   - 采集: ak.stock_board_industry_spot_em() + ak.stock_index_pe_lg()
   - 字段: industry, trade_date, avg_pe, median_pe, up_count, down_count, change_pct
   - 频率: 每日

### Phase B: 选股策略实现

#### 策略1: 趋势突破
```python
# 选股条件:
# - MA5 > MA10 > MA20 > MA60 (均线多头)
# - 今日放量 (量比 > 1.5)
# - 突破近20日高点
# - 非涨停 (涨幅 3%-9%)
```

#### 策略2: 缩量回调
```python
# 选股条件:
# - MA20向上, 股价在MA20之上
# - 近5日缩量 (量比 < 0.7)
# - 回调幅度 < 8%
# - MACD金叉或即将金叉
```

#### 策略3: 资金连续流入
```python
# 选股条件:
# - 主力净流入连续3天 > 0
# - 累计净流入 > 5000万
# - 股价未大涨 (涨幅 < 5%)
```

#### 策略4: 板块轮动
```python
# 选股条件:
# - 板块内龙头首阴/断板
# - 同板块有2只以上跟涨
# - 选取辨识度最高的跟涨标的
```

#### 策略5: 量价筛选
```python
# 选股条件:
# - 近30日成交量温和放大 (量均线递增)
# - PE < 行业平均PE * 0.8
# - 非ST, 非退市风险, 非停牌
```

#### 策略6: 基本面排雷
```python
# 排除条件:
# - 近3年任一年净利润下滑 > 30%
# - 最新ROE < 5%
# - 资产负债率 > 70%
# - 经营现金流连续2年负
# - 商誉/净资产 > 30%
```

#### 策略7: 主力+北向加仓
```python
# 选股条件:
# - 主力近10日净流入 > 0
# - 北向近10日持仓增加
# - 资金流入与股价涨幅匹配 (未明显偏离)
```

#### 策略8: 行业景气
```python
# 选股条件:
# - 行业近5日涨幅 > 大盘
# - 行业PE分位 < 70% (不追高估值)
# - 政策利好板块加分
```

#### 策略9: 风控终审
```python
# 排除条件:
# - 近60日最大回撤 > 30%
# - 30天内有解禁 (解禁比 > 5%)
# - PE分位 > 90% (估值过高)
# - 股权质押比 > 50%
# - 换手率异常 (> 15%)
```

### Phase C: 推荐引擎重构

当前 RecommendEngine._build_candidates() 只从涨停池+强势股池取候选。
改为:

1. **候选池扩展**: 全市场2700只 → 先用Layer 1技术面筛到~200只
2. **分层过滤**: Layer 2-5 逐层过滤
3. **综合打分**: 9个维度加权 (权重可调/自适应)
4. **最终输出**: 5-10只, 附带每只的9维评分明细

### Phase D: 90%胜率保障机制

90%不是靠一次选对，而是靠多层验证 + 严格的买入纪律:

1. **多策略交叉确认**: 同一只股票被≥3个策略选中 → 高置信度
2. **买入价格纪律**: 只在支撑位附近低吸, 不追高
3. **止损止盈**: 严格-3%止损, +5%以上分批止盈
4. **仓位管理**: 单只不超过总仓位35%, 总仓位根据市场状态调整
5. **复盘迭代**: 每日自动复盘, 亏的单子分析原因, 修正策略

## 实施顺序

1. **Phase A** (数据采集) — 先把5张新表建起来
2. **Phase B.1-B.4** (技术面策略) — 用现有数据就能做
3. **Phase B.5-B.9** (基本面/资金/风控) — 等Phase A数据就位
4. **Phase C** (推荐引擎重构) — 整合9维打分
5. **Phase D** (胜率保障) — 回测验证 + 参数调优

## 文件结构

```
src/
  data/
    sources/
      fundamentals.py    ← 新: 基本面采集
      northbound.py      ← 新: 北向资金采集
      valuation.py       ← 新: 估值数据采集
      restricted.py      ← 新: 解禁数据采集
      industry.py        ← 新: 行业数据采集
  strategy/
    screener/            ← 新: 选股器目录
      trend_breakout.py  ← 策略1
      dip_buy.py         ← 策略2
      fund_flow_pick.py  ← 策略3
      sector_rotation.py ← 策略4
      volume_value.py    ← 策略5
      fundamental_filter.py ← 策略6
      smart_money.py     ← 策略7
      industry_momentum.py ← 策略8
      risk_control.py    ← 策略9
    recommend.py         ← 重构: 整合9维
```
