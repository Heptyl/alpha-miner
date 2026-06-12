# 🤖 ML/AI量化学习笔记 — 特征工程深度优化 + HMM市场状态检测方案

> **日期**: 2026-05-12 (周二)  
> **主题**: ML/AI量化 — 特征工程/Alpha挖掘/HMM市场状态检测  
> **学习方式**: 项目代码深度审查 + 经典理论回顾 + HMM实现方案设计  
> **耗时**: 45分钟

---

## 一、学到了什么 — 5个核心知识点

### 1. 特征工程的"流水线思维" — Alpha Miner项目实战总结

**核心认知**: 好的特征工程 = 数据质量 × 因子构建 × 截面处理 × 去噪

Alpha Miner的特征工程管道（`feature_pipeline.py`）已实现了7步流水线：

| 步骤 | 操作 | 当前实现 | 质量评级 |
|------|------|---------|---------|
| 1. 数据加载 | OHLCV从daily_price | ✅ 完整 | A |
| 2. 因子计算 | Alpha158(50+因子) | ✅ 完整 | A |
| 3. 资金流特征 | main_net/net_ratio/main_persist | ✅ 3个 | B |
| 4. 已有因子合并 | factor_values表pivot | ✅ | B |
| 5. 截面标准化 | 行业内rank → 全截面rank | ✅ 双层 | A |
| 6. NaN填充 | 截面均值 → 0 | ✅ | B+ |
| 7. 去极值 | ±3σ clip | ✅ | B+ |

**发现的关键改进点**：

1. **截面标准化过于激进**: 双层rank（先行业内rank，再全截面rank）会压缩信号的区分度。在A股小市值股票多的环境中，行业内排名已经足够，第二层rank会损失信息。
   
2. **资金流特征只有3个**: 仅有`main_net`、`net_ratio`、`main_persist`，缺少以下重要维度：
   - 大单/中单/小单资金分化比
   - 主力资金占成交额比例的5日变化率
   - 尾盘资金流入强度（最后30分钟的净流入）
   
3. **缺少横截面动量因子**: 没有截面动量（过去N天排名前M的股票，未来1天的平均收益）作为市场状态指标

4. **去极值方法简单**: ±3σ clip只处理了极端值，没有处理因子分布的偏度和厚尾。A股因子经常呈现右偏分布，应该用MAD（Median Absolute Deviation）替代标准差

### 2. HMM隐马尔可夫模型 — A股市场状态检测的完整方案

**为什么需要HMM替代当前的简单规则？**

项目当前的市场状态判断（`regime_aware.py`）使用了两种方法：
- 方法1: `market_emotion`表的涨跌比 > 2.0 = bull, < 0.5 = bear
- 方法2: 上证指数20日均线斜率 > 0.02 = bull, < -0.02 = bear

**问题**:
- 阈值固定（0.02/2.0/0.5），无法适应不同波动率环境
- 两种方法可能给出矛盾信号
- 无法识别"过渡状态"（从牛转熊的拐点）
- 对A股的高波动特征不友好

**HMM方案设计**（专为Alpha Miner定制）:

```
观测变量（连续）:
  - 沪深300日收益率
  - 沪深300日波动率(ATR/收盘价)  
  - 全市场涨跌比
  - 全市场涨停家数占比
  - 全市场成交额变化率

隐状态（离散）:
  - State 0: 冰点（极度恐惧）→ 应该空仓或极低仓位
  - State 1: 震荡偏弱 → 谨慎操作，降低仓位
  - State 2: 震荡偏强 → 正常操作
  - State 3: 强势（趋势上行）→ 积极操作，可满仓
  - State 4: 疯狂（过热）→ 减仓止盈，注意风险
```

**HMM的核心优势**:
1. **概率输出**: 不是非此即彼，而是每个状态的概率（如P(bull)=0.6, P(range)=0.3, P(bear)=0.1）
2. **自学习边界**: 不需要人为设定阈值，模型从数据中学到状态切换条件
3. **过渡期识别**: 当多个状态概率接近时（如P(bull)=0.4, P(range)=0.35, P(bear)=0.25），知道市场正处于过渡期

**技术要点**:
- 使用Gaussian HMM（高斯HMM），因为观测变量是连续的
- 用`hmmlearn`库实现，约50行核心代码
- 训练数据: 至少3年（750个交易日）
- EM算法迭代: 50-100次
- 每周重训练一次（增量更新太复杂，全量重训练更稳定）

### 3. Alpha158因子的深度审视 — 哪些因子在A股最有效？

对项目已有的50个Alpha158因子做了分类和价值评估：

**Tier 1 — 核心因子（对A股预测力最强）**:
| 因子 | A股含义 | 为什么有效 |
|------|---------|-----------|
| `ROC_5/10` | 5/10日动量 | A股短线资金驱动，动量效应显著 |
| `VWAP_DEV` | 偏离VWAP程度 | 机构成本线偏离，回归动力 |
| `ATR_RATIO` | 波动率占比 | 高波动=投机股，低波动=价值股 |
| `TCORR_5` | 5日量价相关性 | 量价背离预判反转 |
| `BIAS_5/10` | 均线乖离率 | A股均值回归特性显著 |

**Tier 2 — 有效因子（贡献边际增量）**:
| 因子 | 说明 |
|------|------|
| `MAX_POS_5/10` | N日新高位置 |
| `VSTD_5/10` | 收益率波动率 |
| `BBANDS_WIDTH` | 布林带宽度（收窄=即将变盘） |
| `RSI_6/14` | 超买超卖 |

**Tier 3 — 可能冗余（相关性高，考虑去除）**:
| 因子 | 与谁冗余 |
|------|---------|
| `ROC_60` | 与`MA_60`高度相关 |
| `VSTD_60` | 长期波动率变化太慢 |
| `MIN_POS_*` | = 1 - `MAX_POS_*`，完全冗余 |
| `TURNOVER_D` | 与`VOLUME_D`高度相关 |
| `HIGH_D/LOW_D` | 与`CLOSE_D`信息重叠 |

**关键洞察**: A股的因子有效期通常只有3-6个月，因子衰减很快。项目已有`ic_tracker.py`监控IC，但缺少**自动因子轮换机制**——当某个因子IC连续5天为负时，应该自动降低其权重。

### 4. Purged Cross-Validation — 解决金融ML最大的坑：信息泄露

**问题本质**: 传统的K-Fold CV在金融数据上有致命缺陷。

```
传统K-Fold在时间序列上的问题:

  训练集: [1,2,3,4] [5,6,7,8] [9,10,11,12]
  验证集:    [5,6,7,8] [9,10,11,12] [13,14,15,16]
  
  问题: 训练集第4天和验证集第5天相邻！
        如果用5日动量因子，第5天的因子值包含第1-5天数据，
        而第4天也在训练集中 → 信息泄露！
```

**Purged CV的解决方案**（De Prado, 2018）:

1. **Purge（清洗区）**: 在训练集和验证集之间留出gap
   - gap长度 = 因子的最大回看窗口（如用20日动量，gap=20天）
   - 这样确保训练集的因子不包含验证集的任何信息

2. **Embargo（禁运区）**: 在验证集后面也留gap
   - 防止标签的前瞻偏差

**对Alpha Miner的影响**:
- 项目的`walk_forward.py`实际上已经实现了类似purge的效果（训练窗口和测试窗口不重叠）
- 但`hyperopt_tuner.py`的验证集是从训练集末尾20%切出来的，**没有purge gap**
- **建议**: 在tuner的验证集切分时，至少留5天purge gap

### 5. 特征工程中的A股特殊考虑 — 不只是套公式

**A股T+1制度对特征的影响**:
- 当天买的不能当天卖 → 今天的量价信号反映的是"昨天持有者的行为"
- 因此，用`RET_1D`（次日收益）作为标签是正确的
- 但因子构建时，应该用`shift(1)`的因子值预测`RET_1D`，而不是当天因子预测当天收益
- **项目现状**: `alpha158.py`计算的是当天因子，`labeler.py`用`shift(-1)`的close计算`ret_1d`，这在时间对齐上是正确的 ✅

**涨跌停板对因子的影响**:
- 涨停股的HIGH=LOW=CLOSE=涨停价，K线因子失真
- `KMID = close/open` 对涨停股恒为1.1（或1.2/1.0），无区分度
- **建议**: 增加一个`IS_LIMIT`指示因子，涨停/跌停日单独处理

**A股因子有效性的周期性**:
| 时期 | 有效因子 | 失效因子 |
|------|---------|---------|
| 牛市初期 | 动量、量比 | 价值、低波 |
| 牛市中期 | 趋势、资金流 | 均值回归 |
| 牛市顶部 | 波动率、换手率 | 动量（开始衰减） |
| 熊市 | 低波、价值 | 动量（反转） |
| 震荡市 | 技术形态、量价 | 趋势、动量 |

这正是需要`regime_aware.py`的原因——不同市场状态用不同因子权重。

---

## 二、对Alpha Miner项目的启发

### 2.1 项目ML模块的优势
1. **架构设计好**: `FeaturePipeline → Labeler → WalkForward → HyperoptTuner → RegimeAwareModel` 模块化程度高
2. **Alpha158因子完整**: 50+个因子覆盖了K线、量价、动量、波动率、技术指标
3. **IC监控到位**: `ICTracker`可以实时追踪因子有效性
4. **Regime-Aware已成型**: 分市场状态训练模型的想法是对的，只是状态判断方法太简单

### 2.2 发现的问题与改进方向

| 问题 | 优先级 | 改进方案 |
|------|--------|---------|
| Regime判断用固定阈值，不适应A股高波动 | **P6** | 实现HMM市场状态检测 |
| 特征工程缺少A股特殊处理（涨跌停/T+1） | P7 | 增加涨跌停指示因子+涨停日特殊处理 |
| 截面标准化双层rank损失信号 | P8 | 改为行业内rank后Z-Score，不做第二层rank |
| 去极值用±3σ clip，对偏态分布不友好 | P9 | 改用MAD去极值法 |
| HyperoptTuner验证集无purge gap | P10 | 切分验证集时留5天gap |
| 缺少因子自动轮换机制 | P11 | IC连续5天为负→自动降权 |
| MIN_POS_*因子与MAX_POS_*完全冗余 | P12 | 删除MIN_POS_*系列 |
| 缺少截面动量因子 | P13 | 新增截面动量(momentum cross-section) |

---

## 三、可立即落地的改进建议

### 建议1: 实现P6 — HMM市场状态检测（⭐⭐⭐ 最有价值）

**方案概要**:
```python
# 新文件: src/ml/hmm_regime.py
# 依赖: pip install hmmlearn

from hmmlearn.hmm import GaussianHMM
import numpy as np

class HMMRegimeDetector:
    """基于Gaussian HMM的A股市场状态检测器"""
    
    def __init__(self, n_states=4, db_path="data/alpha_miner.db"):
        self.n_states = n_states
        self.db_path = db_path
        self.model = None
        self.state_labels = {}  # {state_id: "bull"/"bear"/"range"/...}
    
    def _prepare_features(self):
        """从DB准备HMM观测变量"""
        # 5个观测变量:
        # 1. 沪深300日收益率
        # 2. 全市场波动率
        # 3. 涨跌比
        # 4. 涨停家数占比
        # 5. 成交额变化率
        ...
    
    def fit(self, min_days=750):
        """训练HMM模型（至少3年数据）"""
        X = self._prepare_features()
        self.model = GaussianHMM(
            n_components=self.n_states,
            covariance_type="full",
            n_iter=100,
            random_state=42,
        )
        self.model.fit(X)
        self._label_states()  # 根据每个状态的统计特征命名
    
    def predict(self, recent_days=20):
        """预测最近N天的市场状态概率"""
        X = self._get_recent_features(recent_days)
        posteriors = self.model.predict_proba(X)
        # 返回每天的状态概率分布
        return posteriors
    
    def current_regime(self):
        """获取当前市场状态（概率最高的状态+概率）"""
        posteriors = self.predict(recent_days=1)
        state = np.argmax(posteriors[-1])
        prob = posteriors[-1][state]
        return self.state_labels[state], prob
```

**与现有系统的集成点**:
- `regime_aware.py`的`_load_regime_from_db()`方法可以读取HMM的输出
- `emotion_cycle.py`的冰点/高潮判断可以参考HMM的状态概率
- 训练完成后将状态概率写入`regime_state`表

**预期效果**: 
- 更准确的市场状态识别（不需要人为设阈值）
- 识别过渡期（多状态概率接近），减少错误切换
- 与情绪周期模块互补

### 建议2: 优化特征工程管道（3个小改进）

```python
# 改进1: MAD去极值替代±3σ clip
def _winsorize_mad(self, series, n_mad=3):
    """MAD去极值法 — 对偏态分布更鲁棒"""
    median = series.median()
    mad = (series - median).abs().median() * 1.4826  # MAD→标准差换算系数
    lower = median - n_mad * mad
    upper = median + n_mad * mad
    return series.clip(lower, upper)

# 改进2: 涨跌停指示因子
g["IS_LIMIT_UP"] = (g["close"] == g["high"]) & (g["close"] >= g["open"] * 1.095)
g["IS_LIMIT_DOWN"] = (g["close"] == g["low"]) & (g["close"] <= g["open"] * 0.905)

# 改进3: 截面动量因子
# 过去5天截面排名前20%的股票，今天的平均收益
def _cross_section_momentum(self, df, lookback=5):
    """截面动量 — A股特有的'强者恒强'效应"""
    for n in [5, 10]:
        rank = df.groupby("trade_date")["ROC_" + str(n)].rank(pct=True)
        df[f"CS_MOM_{n}"] = rank  # 截面排名本身就是因子
```

### 建议3: HyperoptTuner增加Purge Gap

```python
# 在 _run_wf_backtest 中，验证集切分改为:
val_split = max(1, int(len(X_train_clean) * 0.2))
purge_gap = 5  # 5天清洗区
split_point = len(X_train_clean) - val_split - purge_gap
if split_point < 5:
    continue
X_tr = X_train_clean[:split_point]
y_tr = y_train_clean[:split_point]
X_val = X_train_clean[split_point + purge_gap:]  # 跳过purge_gap
y_val = y_train_clean[split_point + purge_gap:]
```

---

## 四、HMM市场状态检测 — 深入方案（预留给P6实施）

### 4.1 状态数量选择

| 状态数 | 优点 | 缺点 | A股适用性 |
|--------|------|------|---------|
| 3 (bull/bear/range) | 简单，训练快 | 无法区分"冰点"和"正常震荡" | ⭐⭐ |
| 4 (冰点/弱震荡/强震荡/强势) | 覆盖主要状态 | 与情绪周期4阶段对应 | ⭐⭐⭐ 推荐 |
| 5 (冰点/弱/中/强/过热) | 最精细 | 需要更多数据，可能过拟合 | ⭐⭐ |

**推荐**: 4状态，与92科比情绪周期四阶段（冰点→回暖→高潮→退潮）对齐。

### 4.2 训练数据要求
- 最低: 750个交易日（约3年）
- 推荐: 1250个交易日（约5年，覆盖一个完整牛熊周期）
- 更新频率: 每月重训练一次
- BIC/AIC准则: 用贝叶斯信息准则选择最优状态数

### 4.3 与现有模块的集成路线图
```
Phase 1: 独立实现hmm_regime.py + 离线训练 + 回测验证
Phase 2: 将HMM输出写入regime_state表
Phase 3: regime_aware.py读取HMM结果替代简单规则
Phase 4: emotion_cycle.py融合HMM状态概率
Phase 5: 交易策略根据HMM状态自动调整仓位和止损
```

---

## 五、参考资料

- **《Advances in Financial Machine Learning》** (Marcos López de Prado, 2018) — Purged CV、Triple-Barrier标注、HMM金融应用
- **hmmlearn文档** — https://hmmlearn.readthedocs.io/ — Python HMM实现
- **WorldQuant Alpha 101** (Kakushadze, 2016) — 101个Alpha因子公式
- **《因子投资：方法与实践》** (石川) — 第4章因子处理/去极值/标准化
- **华泰金工研报"多因子系列"** — A股因子有效性实证
- **De Prado, M. (2020) "Machine Learning for Asset Managers"** — Chapter 4 聚类与状态检测
- 项目代码: `alpha158.py`, `feature_pipeline.py`, `regime_aware.py`, `walk_forward.py`, `ic_tracker.py`

---

*笔记完成于 2026-05-12 20:45 | Alpha Miner 金牌交易员学习计划*
