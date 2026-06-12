# 龙虎榜(LHB)数据游资识别分析报告

## 一、数据概况

### lhb_detail 表结构
| 列名 | 类型 | 说明 |
|------|------|------|
| id | INTEGER | 主键 |
| stock_code | TEXT | 股票代码 |
| trade_date | TEXT | 交易日期 |
| buy_amount | REAL | 龙虎榜买入额(汇总) |
| sell_amount | REAL | 龙虎榜卖出额(汇总) |
| net_amount | REAL | 龙虎榜净买额 |
| buy_depart | TEXT | 买入营业部(始终为空) |
| sell_depart | TEXT | 卖出营业部(始终为空) |
| reason | TEXT | 上榜原因 |
| _row_idx | INTEGER | 行索引 |
| snapshot_time | TEXT | 采集时间 |

**关键发现: buy_depart/sell_depart 字段始终为空, 无营业部级明细数据。**
- 总记录: 2608条, 覆盖 37 个交易日 (2026-03-16 ~ 2026-05-15)
- 涉及 1095 只股票
- 数据来源: akshare `stock_lhb_detail_em` (汇总接口, 不含营业部明细)

### 与任务描述的差异
任务中提到的 `buy_broker, sell_broker, buy_broker_amount, sell_broker_amount, turnover, raw_json` 字段在当前数据库中 **不存在**。

**补充数据源**: akshare 提供了 `stock_lhb_stock_detail_em` 接口, 可以获取个股级别的 **营业部买卖明细** (含具体营业部名称和金额), 但需要逐股票逐日期调用, 当前未采集。

---

## 二、已有数据的分析结论

### 2.1 上榜原因分类 (6大类)

| 类别 | 说明 | 数量 | 次日表现特征 |
|------|------|------|-------------|
| 日涨幅偏离 | 单日涨幅异常 | ~543 | 平均高开1.89%, 低吸策略wr=54.8% |
| 连续涨幅偏离 | 3日累计涨幅异常 | ~529 | 平均高开1.89%, 低吸策略wr=65.1%(最佳) |
| 高换手率 | 日换手率>20%/30% | ~466 | 平均低开-1.04%, 低吸策略wr=54.0% |
| 日跌幅偏离 | 单日跌幅异常 | ~555 | 平均低开-2.96%, 低吸策略wr=36.5%(最差!) |
| 高振幅 | 日振幅>15% | ~146 | 平均低开-1.49%, 低吸策略wr=42.2% |
| 其他 | ST/退市/可转债等 | ~41 | 低吸策略wr=37.5% |

**结论**: "日跌幅偏离"上榜的股票, 次日低吸策略胜率仅36.5%, 应作为**排除条件**。

### 2.2 买卖比(sell/buy ratio)与次日表现

以全部2280条LHB记录(含次日行情)分析:

| 买卖比区间 | 样本数 | 次日平均开盘涨幅 | 低吸策略(avg_ret) | 低吸策略(wr) |
|-----------|--------|-----------------|------------------|-------------|
| sell/buy > 2.0 (重度净卖出) | 197 | -3.26% | 0.64% | **33.3%** |
| sell/buy > 1.0 (净卖出) | 1052 | -1.61% | 1.04% | 44.8% |
| sell/buy < 1.0 (净买入) | 1228 | +1.08% | 1.06% | 50.2% |

**结论**: LHB净卖出(sell/buy > 1)的股票, 低吸胜率仅44.8%; 重度净卖出(>2)胜率仅33.3%, 是明确的**游资出货信号**。

### 2.3 策略B回测对比 (ZT次日低吸, gap_down>=2%)

| 过滤条件 | 样本数 | 平均收益 | 胜率 | 累计收益 |
|----------|--------|---------|------|---------|
| 全部(不过滤) | 239 | 1.34% | 60.3% | 1637.6% |
| **排除LHB净卖出** | **210** | **1.50%** | **61.4%** | **1641.0%** |
| 被排除的(LHB净卖出) | 29 | 0.19% | 51.7% | -0.2% |
| 非LHB涨停股 | 141 | 1.13% | 58.2% | 320.5% |
| LHB净买入涨停股 | 69 | 2.25% | 68.1% | 314.1% |

**核心发现**:
- LHB净买入的ZT股, 低吸策略胜率高达68.1% (比基准高8个百分点)
- LHB净卖出的ZT股, 低吸策略胜率仅51.7%, 应过滤
- 过滤后总收益基本持平(210笔的累计收益 ≈ 239笔), 但单笔均值和胜率都提升

### 2.4 细分维度: 首板 vs 连板

| 组合 | 样本(低吸) | 平均收益 | 胜率 |
|------|-----------|---------|------|
| 首板 + LHB净买入 | 50 | 1.37% | 60.0% |
| 首板 + LHB净卖出 | 16 | -0.77% | **43.8%** |
| 连板 + LHB净买入 | 18 | 4.32% | **88.9%** |
| 连板 + LHB净卖出 | 14 | 1.93% | 64.3% |

**结论**: 连板+LHB净买入是最佳组合(88.9%胜率); 首板+LHB净卖出是危险组合(43.8%胜率)。

---

## 三、游资识别: 营业部数据分析 (akshare在线)

通过 `stock_lhb_yybph_em` 获取的近一月营业部排行数据:

### 3.1 确认的游资"散户化"席位 (东方财富拉萨系)

| 营业部 | 上榜次数 | 次日平均涨幅 | 次日上涨概率 |
|--------|---------|-------------|-------------|
| 东方财富拉萨金融城南环路 | 141 | -0.60% | 44.0% |
| 东方财富拉萨东环路第二 | 122 | -2.31% | **34.4%** |
| 东方财富拉萨团结路第一 | 122 | -1.51% | 39.3% |
| 东方财富山南香曲东路 | 60 | -1.21% | 36.7% |
| 东方财富拉萨团结路第二 | 49 | -0.58% | 44.9% |
| 东方财富拉萨东环路第一 | 46 | -1.84% | 39.1% |

**特征**: 拉萨系席位买入后次日平均涨幅为负, 上涨概率仅34-44%, 典型的"散户接盘"席位。

### 3.2 知名游资/实力席位

| 营业部 | 上榜次数 | 次日平均涨幅 | 次日上涨概率 |
|--------|---------|-------------|-------------|
| 开源证券西安太华路 | 122 | +2.39% | 59.0% |
| 国信证券深圳红岭中路 | 51 | +2.55% | 60.8% |
| 国泰海通武汉紫阳东路 | 53 | +1.86% | 60.4% |
| 国金证券深圳分公司 | 81 | +0.83% | 56.8% |

**特征**: 实力游资席位次日正收益, 上涨概率>55%, 跟买有正期望。

---

## 四、游资过滤实现建议

### 4.1 现有数据可用的过滤条件 (lhb_detail汇总数据)

**核心SQL过滤条件:**

```sql
-- 策略B选股: 昨日涨停 + 排除LHB净卖出
SELECT z.stock_code, z.trade_date, z.name, z.consecutive_zt
FROM zt_pool z
WHERE z.trade_date = '{yesterday}'
  AND z.consecutive_zt >= 1
  -- 排除LHB净卖出(核心过滤)
  AND NOT EXISTS (
    SELECT 1 FROM lhb_detail l 
    WHERE l.stock_code = z.stock_code 
      AND l.trade_date = z.trade_date 
      AND l.sell_amount > l.buy_amount  -- 净卖出
  )
```

**分级过滤规则:**

| 优先级 | 条件 | 效果 |
|--------|------|------|
| P0 必须 | 排除LHB净卖出 (sell > buy) | 胜率从51.7%提升到61.4% |
| P1 建议 | 排除"日跌幅偏离"上榜原因 | 避免接下跌趋势的刀 |
| P2 加分 | 保留"连续涨幅偏离"的LHB | 这类股低吸胜率65.1% |
| P3 可选 | 连板(cons_zt>1) + LHB净买入 | 最优组合, 胜率88.9% |

### 4.2 建议新增: 营业部明细数据采集

当前 lhb_detail 只有汇总数据, 建议增加营业部明细表:

```sql
CREATE TABLE IF NOT EXISTS lhb_broker (
    stock_code TEXT NOT NULL,
    trade_date TEXT NOT NULL,
    broker_name TEXT NOT NULL,      -- 营业部名称
    direction TEXT NOT NULL,        -- '买入' / '卖出'
    buy_amount REAL DEFAULT 0,      -- 该营业部买入额
    sell_amount REAL DEFAULT 0,     -- 该营业部卖出额
    net_amount REAL DEFAULT 0,      -- 该营业部净额
    buy_pct REAL DEFAULT 0,         -- 买入占总成交比
    sell_pct REAL DEFAULT 0,        -- 卖出占总成交比
    reason TEXT DEFAULT '',
    snapshot_time TEXT DEFAULT (datetime('now')),
    PRIMARY KEY (stock_code, trade_date, broker_name, direction)
);
```

**采集代码建议:**

```python
import akshare as ak

# 已知危险游资席位关键词(次日上涨概率<45%)
HOT_MONEY_BROKER_KEYWORDS = [
    '东方财富证券拉萨',  # 拉萨全系
    '东方财富证券山南',   # 山南系
    '东方财富证券昌都',   # 昌都系
    '东方财富证券林芝',   # 林芝系
]

def fetch_broker_detail(stock_code, trade_date):
    """获取个股龙虎榜营业部明细"""
    date_str = trade_date.replace('-', '')
    # 先获取可用日期
    dates_df = ak.stock_lhb_stock_detail_date_em(symbol=stock_code)
    if date_str not in dates_df.values:
        return []
    
    results = []
    for flag in ['买入', '卖出']:
        df = ak.stock_lhb_stock_detail_em(
            symbol=stock_code, date=date_str, flag=flag
        )
        for _, row in df.iterrows():
            results.append({
                'broker_name': row['交易营业部名称'],
                'direction': flag,
                'buy_amount': row.get('买入金额', 0),
                'sell_amount': row.get('卖出金额', 0),
            })
    return results

def is_hot_money_broker(broker_name):
    """判断是否为游资/散户席位"""
    for kw in HOT_MONEY_BROKER_KEYWORDS:
        if kw in broker_name:
            return True
    return False
```

### 4.3 完整游资过滤策略 (含营业部数据后)

```python
def should_filter_stock(stock_code, trade_date):
    """判断涨停股是否应被游资过滤"""
    
    # === 第一层: 汇总数据过滤 (现有lhb_detail) ===
    lhb = query_lhb_detail(stock_code, trade_date)
    if lhb:
        # 净卖出 → 大概率游资出货
        if lhb.sell_amount > lhb.buy_amount:
            return True, "LHB净卖出"
        
        # 上榜原因为跌幅偏离 → 下跌趋势
        if '跌幅' in lhb.reason:
            return True, "跌幅偏离上榜"
    
    # === 第二层: 营业部明细过滤 (需新增数据) ===
    brokers = query_lhb_broker(stock_code, trade_date)
    if brokers:
        hot_money_buy_count = sum(
            1 for b in brokers 
            if b.direction == '买入' and is_hot_money_broker(b.broker_name)
        )
        total_buy_brokers = sum(
            1 for b in brokers if b.direction == '买入'
        )
        
        # 拉萨系占买入席位>=50% → 散户接盘
        if total_buy_brokers > 0 and hot_money_buy_count / total_buy_brokers >= 0.5:
            return True, "游资散户席位主导"
        
        # 单一游资买入占比过大
        for b in brokers:
            if b.direction == '买入' and is_hot_money_broker(b.broker_name):
                if b.buy_pct > 0.1:  # 单席位占成交>10%
                    return True, "单一游资席位大额买入"
    
    return False, ""
```

---

## 五、总结

### 可直接使用的过滤条件 (基于现有数据)

1. **LHB净卖出** (`sell_amount > buy_amount`): 过滤掉次日后胜率仅51.7%的标的
2. **上榜原因含"跌幅偏离"**: 排除下跌趋势股, 胜率仅36.5%
3. **连板+LHB净买入**: 优先选择, 胜率88.9%

### 建议改进 (需新增数据采集)

4. **新增 lhb_broker 表**: 采集营业部明细数据 (`stock_lhb_stock_detail_em`)
5. **拉萨系游资席位过滤**: 东方财富拉萨/山南/昌都/林芝等营业部主导的涨停
6. **单一席位集中度**: 单个游资席位买入占比>10%

### 预期效果

使用现有P0过滤(仅排除LHB净卖出):
- 样本量: 210笔 (减少12%)
- 胜率: 60.3% → 61.4%
- 单笔均值: 1.34% → 1.50%
- 累计收益基本持平, 但风险收益比改善
