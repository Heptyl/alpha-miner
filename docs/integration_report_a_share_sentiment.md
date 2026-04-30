# a-share-sentiment 技能包 → Alpha Miner 集成评估报告

## 一、技能包脚本功能概览

### 1. board_attack.py — 短线打板分析
- **功能**: 市场情绪 → 涨停梯队 → 强势股池 → 炸板分析 → 打板候选评分
- **akshare 接口**:
  - `stock_market_activity_legu()` — 市场活跃度（涨停/跌停/活跃度%）
  - `stock_zt_pool_em(date)` — 涨停池（连板数、封板资金、炸板次数）
  - `stock_zt_pool_strong_em(date)` — 强势股票池
  - `stock_zt_pool_zbgc_em(date)` — 炸板股池
- **核心公式**: `连板质量评分 = 连板数×3 + 封板资金(亿)×1 - 炸板次数×5`
- **核心逻辑**: `emotion_level()` — 基于涨停家数+活跃度的5级情绪分级

### 2. dragon_tiger.py — 龙虎榜 + 游资动向
- **功能**: 获取龙虎榜明细 → 识别知名游资席位 → 按个股聚合 → 游资动向汇总
- **akshare 接口**: `stock_lhb_detail_em(start_date, end_date)`
- **核心逻辑**:
  - 15个知名游资席位映射（赵老哥、章盟主、拉萨天团等）
  - `match_famous_trader()` — 席位识别
  - `aggregate_by_stock()` — 按个股聚合买卖净额
- **输出**: JSON/text 双格式，支持 --date 参数

### 3. retail_sentiment.py — 散户情绪指标
- **功能**: 涨停/跌停/炸板统计 → 情绪温度计 → 连板统计
- **akshare 接口**:
  - `stock_zt_pool_em()` — 涨停池
  - `stock_zt_pool_dtgc_em()` — 跌停池（**新增接口**）
  - `stock_zt_pool_strong_em()` — 强势股池
  - `stock_zt_pool_zbgc_em()` — 炸板股池
- **核心公式**: `情绪温度 = 50 + min(涨停×0.5, 30) - min(跌停×1.5, 30) - min(炸板×0.8, 15)`
- **输出**: 温度值(0-100) + 5级情绪标签（极度贪婪→极度恐慌）

### 4. stock_news_sentiment.py — 个股新闻情感分析
- **功能**: 获取个股新闻 → SnowNLP 情感打分 → 利好/利空关键词标记
- **akshare 接口**: `stock_news_em(symbol)`
- **核心逻辑**:
  - SnowNLP 情感打分 + 关键词修正（23个利好词 + 21个利空词）
  - `summarize()` — 统计汇总（利好/利空/中性计数 + 综合倾向）

---

## 二、功能重叠与差异对比

### 完全重叠（已有，无需集成）

| 技能包功能 | Alpha Miner 对应模块 | 说明 |
|---|---|---|
| 涨停池获取 `stock_zt_pool_em` | `src/data/sources/akshare_zt_pool.py` | AM 版本更完善，有重试+存储 |
| 强势股池 `stock_zt_pool_strong_em` | `src/data/sources/akshare_zt_pool.py` | AM 版本更完善 |
| 炸板池 `stock_zt_pool_zbgc_em` | `src/data/sources/akshare_zt_pool.py` | AM 版本更完善 |
| 龙虎榜 `stock_lhb_detail_em` | `src/data/sources/akshare_lhb.py` | AM 版本有去重+存储 |
| 个股新闻 `stock_news_em` | `src/data/sources/akshare_news.py` | AM 版本有 news_id 去重+分类 |
| 新闻情感分析 SnowNLP | `src/data/sources/akshare_news.py` `_sentiment()` | 实现完全一致 |
| 连板数因子 | `src/factors/formula/consecutive_board.py` | AM 已有 |
| 龙虎榜因子 | `src/factors/formula/lhb_institution.py` | AM 已有 |
| 市场活跃度 `stock_market_activity_legu` | `src/data/collector.py` line 187 | AM 已在采集器中使用 |

### 部分重叠（有增量价值）

| 技能包功能 | Alpha Miner 现状 | 增量价值 |
|---|---|---|
| 情绪分级 (5级) | AM 有 `_market_regime()` 简单3级 | 技能包5级更精细，含活跃度指标 |
| 连板质量评分 | AM 有 `consecutive_board` 因子(仅连板数) | 技能包加入封板资金和炸板惩罚 |
| 涨停/跌停比 | AM 有 `zt_ratio.py` 市场因子 | 逻辑类似 |

### 全新功能（Alpha Miner 尚无）

| 技能包功能 | 说明 | 集成价值 |
|---|---|---|
| **跌停池** `stock_zt_pool_dtgc_em` | 直接获取跌停股列表 | ★★★ 高 — 免去从 K 线推算跌停的麻烦 |
| **情绪温度计公式** | 基于涨/跌/炸板计分的 0-100 温度 | ★★★ 高 — 可直接作为市场级因子 |
| **游资席位识别** | 15个知名游资席位映射 | ★★★ 高 — AM 龙虎榜模块完全缺失此功能 |
| **利好/利空关键词库** | 44个金融关键词(23+21) | ★★☆ 中 — AM 新闻模块缺少此规则层 |
| **热点权重评分框架** | 5维度加权评分(题材催化剂30%+连板厚度25%+板块共振20%+持续天数15%+炸板10%) | ★★★ 高 — 可作为新因子 |
| **题材生命周期四阶段** | 启动→爆发→高潮→衰退 | ★★☆ 中 — AM 的 theme_lifecycle 因子概念类似 |

---

## 三、集成建议

### 优先级 P0（高价值，建议立即集成）

#### 1. 新因子: `board_quality` — 连板质量评分
- **公式**: `连板数×3 + 封板资金(亿)×1 - 炸板次数×5`
- **来源**: board_attack.py 的 `calc_quality_score()`
- **集成方式**:
  - 在 `src/factors/formula/` 新建 `board_quality.py`
  - 数据源: zt_pool 表已有 consecutive_zt、open_count，需新增封板资金(seal_amount)
  - zt_pool 表需增加 `seal_amount` 字段（从 akshare 的封板资金列获取）
  - 归一化: score / 15 → 0-1（15分对应5星）
- **集成到推荐引擎**: 在 `DEFAULT_WEIGHTS` 中加入 `"board_quality": 0.10`

#### 2. 新因子: `sentiment_temperature` — 市场情绪温度
- **公式**: `50 + min(涨停×0.5, 30) - min(跌停×1.5, 30) - min(炸板×0.8, 15)`
- **来源**: retail_sentiment.py 的 `calc_sentiment_temperature()`
- **集成方式**:
  - 在 `src/factors/formula/` 新建 `sentiment_temperature.py`
  - 数据源: zt_pool + zb_pool + daily_price(推算跌停)
  - 类型: `factor_type = "market"` — 作为市场环境调节因子
  - 不直接参与个股打分，而是作为推荐引擎的仓位/风险调节器
- **集成到推荐引擎**: 在 `recommend()` 中读取该因子，调节信号等级阈值

#### 3. 新数据源: 跌停池采集
- **接口**: `stock_zt_pool_dtgc_em(date)`
- **来源**: retail_sentiment.py
- **集成方式**: 在 `src/data/sources/akshare_zt_pool.py` 新增 `fetch_dt_pool()` + `save_dt_pool()`
- **价值**: 直接获取跌停股列表，免去从 K 线推算的复杂逻辑

#### 4. 龙虎榜游资席位识别
- **来源**: dragon_tiger.py 的 `FAMOUS_TRADERS` + `match_famous_trader()`
- **集成方式**:
  - 将 15 个游资席位映射移入 `src/data/sources/akshare_lhb.py`
  - 在 LHB 采集时自动标注游资身份 → 存入 `famous_trader` 字段
  - 增强 `lhb_institution.py` 因子：区分机构买入 vs 知名游资买入
- **价值**: 游资动向是短线重要信号，当前 AM 完全缺失

### 优先级 P1（中价值，建议近期集成）

#### 5. 新闻关键词增强
- **来源**: stock_news_sentiment.py 的 `POSITIVE_KEYWORDS` + `NEGATIVE_KEYWORDS`
- **集成方式**: 将 44 个关键词加入 `src/data/sources/akshare_news.py` 的情感分析流程
- **价值**: 增强 SnowNLP 对金融文本的判断力（SnowNLP 通用情感对金融领域不敏感）

#### 6. 热点权重评分 → 增强 `theme_crowding` 因子
- **来源**: SKILL.md 的热点权重评分框架
- **5 维度**: 题材催化剂(30%) + 连板厚度(25%) + 板块共振(20%) + 持续天数(15%) + 炸板(-10%)
- **集成方式**: 将这 5 个维度融入现有的 `theme_crowding` 或 `theme_lifecycle` 因子计算中
- **注意**: 其中"题材催化剂强度"需要人工/LLM 标注，可先用其他 4 个维度

### 优先级 P2（低优先级，作为参考）

#### 7. 5 级情绪分级 → 替换 3 级市场状态
- **来源**: board_attack.py 的 `emotion_level()`
- **集成方式**: 替换 `recommend.py` 的 `_market_regime()` — 从 3 级(强势/弱势/震荡)升级为 5 级
- **价值**: 更精细的市场状态描述

#### 8. 炸板二打候选逻辑
- **来源**: board_attack.py 对"炸板1次可关注二打"的分析
- **集成方式**: 在推荐引擎的 `_build_candidates()` 中增加炸板回封候选

---

## 四、关于"热点权重评分框架"和"连板质量评分公式"能否作为新因子

### 连板质量评分公式 — ✅ 可以直接作为新因子

| 评估项 | 结论 |
|---|---|
| 公式明确性 | ✅ `连板数×3 + 封板资金(亿)×1 - 炸板次数×5`，纯数学公式，可精确复现 |
| 数据可得性 | ✅ zt_pool 已有连板数和炸板次数，封板资金需新增采集 |
| 时间隔离 | ✅ 均为当日收盘后数据，无未来数据风险 |
| 因子命名建议 | `board_quality` |
| 预估IC | 需回测验证，但逻辑上连板数+封板资金-炸板是短线强度的良好代理 |

### 热点权重评分框架 — ⚠️ 部分可因子化，需拆解

| 维度 | 能否因子化 | 说明 |
|---|---|---|
| 题材催化剂强度(30%) | ❌ 需人工/LLM判断 | 可用新闻情感聚合近似替代 |
| 连板梯队厚度(25%) | ✅ 可因子化 | 统计板块内连板≥3的数量 + 1板跟风家数 |
| 板块共振广度(20%) | ✅ 可因子化 | 统计同题材涨停≥5家的板块数 |
| 题材持续天数(15%) | ✅ 可因子化 | 概念板块连续涨停天数 |
| 炸板次数(-10%) | ✅ 可因子化 | 已有数据 |

**建议**: 将后 4 个维度做成复合因子 `hotspot_momentum`，题材催化剂暂不纳入。

---

## 五、集成路线图

```
Phase 1 (本周): 数据层补全
├── zt_pool 增加 seal_amount 字段
├── 新增 dt_pool (跌停池) 采集
└── 龙虎榜增加 famous_trader 标注

Phase 2 (本周): 新因子
├── board_quality (连板质量评分)
├── sentiment_temperature (市场情绪温度)
└── 更新推荐引擎权重

Phase 3 (下周): 增强现有因子
├── 新闻关键词增强
├── lhb_institution 增加游资维度
└── hotspot_momentum 复合因子(可选)

Phase 4 (下周): 回测验证
└── 对新因子做 IC/ICIR/胜率 验证，确认达到验收标准
```

---

## 六、总结

| 维度 | 结论 |
|---|---|
| 数据源重叠度 | **高** (~80%) — 核心接口已覆盖 |
| 逻辑增量价值 | **中高** — 游资识别、连板质量评分、情绪温度计是真正的增量 |
| 集成难度 | **低** — 技能包逻辑简单直接，AM 架构可无缝接入 |
| 建议策略 | **抽取精华，不整体移植** — 只取 3 个新因子 + 游资识别 + 跌停池 |
