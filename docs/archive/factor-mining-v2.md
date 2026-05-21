# 因子挖掘引擎 v2 — 重新设计

> 替换原 Phase 4。本设计解决三个问题：
> 1. 因子空间太窄（不只是公式组合）
> 2. 没有进化机制（失败了就扔掉）
> 3. 没有利用用户的实盘认知

---

## 核心思路转变

原设计：LLM 猜公式 → 回测 IC → 接受/拒绝（独立随机尝试）

新设计：**知识驱动的假说树 + 条件组合因子 + 进化淘汰**

```
                    ┌─────────────────────┐
                    │   知识库（你的认知）    │
                    │  信息瀑布模型          │
                    │  三班组识别规则         │
                    │  题材生命周期理论       │
                    │  封板单预警读法         │
                    │  实盘复盘记录          │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  假说生成器            │
                    │  不是随机猜             │
                    │  而是从知识库推导        │
                    │  "如果瀑布理论正确,     │
                    │   那么 X 应该能预测 Y"  │
                    └──────────┬──────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        ┌──────────┐   ┌──────────┐   ┌──────────┐
        │ 公式因子  │   │ 条件因子  │   │ 交叉因子  │
        │ f(price) │   │ if A & B │   │ f×g      │
        └────┬─────┘   └────┬─────┘   └────┬─────┘
             │              │              │
             └──────────────┼──────────────┘
                            ▼
                    ┌──────────────────┐
                    │  回测验证          │
                    │  IC + 胜率 + 盈亏比 │
                    └──────────┬───────┘
                               │
                    ┌──────────▼──────────┐
                    │  进化引擎            │
                    │  失败原因分析         │
                    │  参数变异            │
                    │  条件增减            │
                    │  因子杂交            │
                    └──────────┬──────────┘
                               │
                       ┌───────┴───────┐
                       ▼               ▼
                    接受入库         带反馈重试
```

---

## 一、知识库（knowledge_base/）

将你在 a-share-sentiment SKILL.md 中已有的认知结构化为可检索的假说种子。

### 1.1 创建知识条目文件

```yaml
# knowledge_base/theories.yaml

theories:
  - id: info_cascade
    name: "信息瀑布模型"
    source: "BHW 1992 + 实盘验证"
    core_claim: "涨停板触发全市场注意力阈值信号，后续观察者放弃私有判断跟随公共信号"
    testable_predictions:
      - id: cascade_momentum
        prediction: "首次涨停后，如果封单稳定，次日高开概率显著高于随机"
        factor_type: conditional
        conditions: ["首次涨停", "封板未开过", "流通市值>20亿"]
        target: "次日收益率"
        
      - id: cascade_break_crash
        prediction: "连板股断板后，反向瀑布导致跌幅远超正常调整"
        factor_type: conditional
        conditions: ["前日连板>=3", "今日炸板"]
        target: "未来3日收益率（应为负）"
        
      - id: seal_decay_warning
        prediction: "封板单量连续下降是断板前兆，领先断板1-2日"
        factor_type: formula
        expression: "封板单量 3日变化率"
        target: "次日是否炸板"

  - id: three_shift
    name: "三班组手法"
    source: "深华发A复盘"
    core_claim: "游资分三班协作拉升出货，识别信号可规避天地板"
    testable_predictions:
      - id: small_cap_trap
        prediction: "小市值+低换手+无题材的连板股，天地板概率极高"
        factor_type: conditional
        conditions: ["流通市值<20亿", "换手率<10%", "无明确题材", "连板>=3"]
        target: "未来3日最大跌幅"
        
      - id: fund_flow_diverge_exit
        prediction: "超大单买+大单卖的背离出现在高位连板时，是出货信号"
        factor_type: conditional
        conditions: ["连板>=3", "超大单净流入>0", "大单净流出>0"]
        target: "次日收益率（应为负）"

  - id: theme_lifecycle
    name: "题材生命周期"
    source: "计划中的四阶段模型"
    core_claim: "题材有启动→爆发→高潮→衰退的生命周期，不同阶段策略不同"
    testable_predictions:
      - id: early_theme_alpha
        prediction: "题材启动期（1-2日，涨停1-3家）买入，未来5日收益最高"
        factor_type: conditional
        conditions: ["所属题材连续涨停天数<=2", "题材涨停家数<=3"]
        target: "未来5日收益率"
        
      - id: crowded_theme_decay
        prediction: "题材拥挤度>30%（占全市场涨停比例）时，题材即将见顶"
        factor_type: formula
        expression: "题材涨停家数 / 全市场涨停家数"
        target: "未来3日题材平均收益"
        
      - id: narrative_exhaustion
        prediction: "龙头高位换手率暴增+股价不创新高=出货，题材衰退"
        factor_type: conditional
        conditions: ["是题材龙头", "换手率>前5日均值200%", "未创近3日新高"]
        target: "未来3日收益率"

  - id: emotion_regime
    name: "情绪驱动的策略切换"
    source: "情绪判断框架"
    core_claim: "不同情绪级别下，最优策略截然不同"
    testable_predictions:
      - id: strong_emotion_board_alpha
        prediction: "情绪极强时（涨停>80家），追高位龙头仍有正收益"
        factor_type: conditional
        conditions: ["全市场涨停>80家", "个股连板>=3"]
        target: "次日收益率"
        
      - id: weak_emotion_avoid
        prediction: "情绪极弱时（涨停<20家），任何打板策略都是负期望"
        factor_type: conditional
        conditions: ["全市场涨停<20家"]
        target: "涨停股次日平均收益（应为负或极低）"
        
      - id: emotion_reversal
        prediction: "情绪极弱持续3日以上，第4日开始涨停股次日收益回升"
        factor_type: conditional
        conditions: ["连续3日涨停<20家", "今日涨停家数开始回升"]
        target: "次日涨停股平均收益"
```

### 1.2 知识库的作用

不是让 LLM 从零猜，而是让它：
1. 从 theories.yaml 中选择一个 testable_prediction
2. 将其转化为可计算的因子
3. 回测验证该预测是否成立
4. 如果不成立，分析原因（条件太宽？阈值不对？时间窗口不对？）
5. 调整后重试

这样每一轮挖掘都有理论指导，不是盲目搜索。

---

## 二、三种因子类型

原设计只有公式因子。新设计支持三种：

### 2.1 公式因子（和原来一样）

```python
# 纯数学变换，输出连续值
# 例：换手率分位数、涨停跌停比
class FormulaFactor(BaseFactor):
    def compute(self, universe, as_of, db) -> pd.Series:
        # 返回 stock_code -> float
```

### 2.2 条件因子（新增，核心创新）

```python
# 多条件组合，输出 0/1 或条件满足的强度
# 例："小市值 + 低换手 + 无题材 + 连板>=3" → 天地板风险信号
class ConditionalFactor(BaseFactor):
    """
    条件因子：将多个离散条件组合成一个信号。
    
    与公式因子的区别：
    - 公式因子是连续映射 f(x) -> R
    - 条件因子是逻辑组合 (A and B and C) -> {0, 1} 或满足条件数/总条件数
    
    为什么需要：A股短线的 alpha 往往在条件交叉处。
    单看"小市值"没有预测力，单看"连板"也没有，
    但"小市值 + 低换手 + 连板>=3 + 无题材" 组合起来天地板概率极高。
    """
    
    conditions: list[Condition]  # 条件列表
    logic: str = "all"  # "all"=全部满足, "any"=任一满足, "count"=满足条件计数
    
    def compute(self, universe, as_of, db) -> pd.Series:
        results = pd.DataFrame(index=universe)
        for cond in self.conditions:
            results[cond.name] = cond.evaluate(universe, as_of, db)
        
        if self.logic == "all":
            return results.all(axis=1).astype(float)
        elif self.logic == "count":
            return results.sum(axis=1) / len(self.conditions)
        elif self.logic == "any":
            return results.any(axis=1).astype(float)


class Condition:
    """单个条件，可复用于多个条件因子。"""
    name: str
    table: str
    column: str
    operator: str  # ">", "<", ">=", "<=", "==", "in", "between"
    value: Any
    
    def evaluate(self, universe, as_of, db) -> pd.Series:
        """返回 stock_code -> bool"""
```

### 2.3 交叉因子（新增）

```python
# 两个已有因子的乘积/比值/条件组合
# 例：theme_lifecycle × seal_success_rate（题材启动期 + 封板质量高）
class CrossFactor(BaseFactor):
    """
    交叉因子：组合两个已有因子。
    
    为什么需要：因子之间的交互效应可能比单因子更强。
    题材启动期（lifecycle=0.8）的股票里，封板质量也高的那些，
    次日收益显著高于只看单一因子。
    """
    
    factor_a: str  # 已有因子名
    factor_b: str  # 已有因子名
    operation: str  # "multiply", "divide", "max", "conditional"
    
    def compute(self, universe, as_of, db) -> pd.Series:
        registry = FactorRegistry()
        a = registry.get_factor(self.factor_a).compute(universe, as_of, db)
        b = registry.get_factor(self.factor_b).compute(universe, as_of, db)
        
        if self.operation == "multiply":
            return a * b
        elif self.operation == "divide":
            return a / b.replace(0, np.nan)
        elif self.operation == "conditional":
            # a > 中位数时才用 b，否则为 0
            return b.where(a > a.median(), 0)
```

---

## 三、进化引擎（关键差异）

原设计：失败 → 扔掉 → 下一轮从零开始
新设计：失败 → 分析原因 → 定向变异 → 重试

### 3.1 失败原因分析器

```python
# src/mining/failure_analyzer.py

class FailureAnalyzer:
    """分析因子回测失败的具体原因，指导下一轮迭代。"""
    
    def analyze(self, factor, backtest_result) -> dict:
        """
        返回结构化失败原因 + 建议调整方向。
        
        可能的失败模式：
        
        1. IC 为零 → 因子无信息量
           原因可能：条件太宽（几乎所有股票都满足）或太窄（样本太少）
           建议：收紧/放宽条件阈值
           
        2. IC 为正但不稳定（ICIR < 0.5）→ 因子有信号但噪音大
           原因可能：某些 regime 下有效，其他无效
           建议：加 regime 条件过滤
           
        3. IC 先正后负 → 因子曾经有效但已衰减
           原因可能：市场结构变化、因子被套利掉
           建议：标记为"regime-dependent"，只在特定状态启用
           
        4. IC 为负 → 因子方向反了
           建议：反转因子方向重新测试
           
        5. 与已有因子高相关 → 信息重复
           建议：找到高相关的已有因子，做差异化（比如加额外条件）
           
        6. 样本量不足 → 条件太严格
           建议：放宽条件
        """
        
        result = {}
        ic = backtest_result['ic_mean']
        icir = backtest_result['icir']
        sample = backtest_result['avg_sample_per_day']
        correlation = backtest_result['max_correlation']
        ic_series = backtest_result['ic_series']
        
        # 诊断
        if abs(ic) < 0.01:
            if sample < 5:
                result['diagnosis'] = 'too_strict'
                result['suggestion'] = '放宽条件阈值，增加样本量'
            elif sample > 100:
                result['diagnosis'] = 'too_loose'
                result['suggestion'] = '收紧条件，当前条件无区分度'
            else:
                result['diagnosis'] = 'no_signal'
                result['suggestion'] = '假说可能不成立，尝试不同维度'
                
        elif ic > 0.01 and icir < 0.5:
            # 检查是否在某些时段有效
            ic_positive_pct = (ic_series > 0).mean()
            if ic_positive_pct > 0.6:
                result['diagnosis'] = 'noisy_but_directional'
                result['suggestion'] = '加入 regime 过滤条件，只在情绪强势时启用'
            else:
                result['diagnosis'] = 'inconsistent'
                result['suggestion'] = '信号不稳定，尝试增加平滑窗口或更换时间窗口'
                
        elif ic < -0.01:
            result['diagnosis'] = 'reversed'
            result['suggestion'] = '反转因子方向'
            
        elif correlation > 0.7:
            result['diagnosis'] = 'redundant'
            result['correlated_with'] = backtest_result['most_correlated_factor']
            result['suggestion'] = f'与 {result["correlated_with"]} 高度相关，加入差异化条件'
        
        # 分段 IC 分析
        ic_first_half = ic_series[:len(ic_series)//2].mean()
        ic_second_half = ic_series[len(ic_series)//2:].mean()
        if ic_first_half > 0.03 and ic_second_half < 0.01:
            result['decay_detected'] = True
            result['suggestion'] += '；因子存在衰减，可能需要更短的回看窗口'
            
        return result
```

### 3.2 变异操作器

```python
# src/mining/mutator.py

class FactorMutator:
    """对失败因子进行定向变异。"""
    
    def mutate(self, factor_config: dict, failure_analysis: dict) -> list[dict]:
        """根据失败原因生成 2-3 个变异版本。
        
        变异类型：
        1. 阈值变异：条件中的数值 ±20%、±50%
        2. 窗口变异：lookback_days 变为 0.5x 或 2x
        3. 条件增删：从知识库补充或移除一个条件
        4. 方向反转：因子值取反
        5. Regime 过滤：加入市场状态前置条件
        6. 因子杂交：与另一个有效因子做交叉
        """
        
        mutations = []
        diagnosis = failure_analysis.get('diagnosis')
        
        if diagnosis == 'too_strict':
            # 放宽阈值
            mutations.append(self._loosen_thresholds(factor_config, ratio=0.8))
            mutations.append(self._remove_weakest_condition(factor_config))
            
        elif diagnosis == 'too_loose':
            # 收紧阈值
            mutations.append(self._tighten_thresholds(factor_config, ratio=1.2))
            mutations.append(self._add_condition_from_knowledge(factor_config))
            
        elif diagnosis == 'reversed':
            mutations.append(self._reverse_direction(factor_config))
            
        elif diagnosis == 'noisy_but_directional':
            mutations.append(self._add_regime_filter(factor_config, 'strong_emotion'))
            mutations.append(self._add_regime_filter(factor_config, 'theme_rotation'))
            
        elif diagnosis == 'redundant':
            corr_factor = failure_analysis.get('correlated_with')
            mutations.append(self._differentiate_from(factor_config, corr_factor))
            
        return mutations
```

### 3.3 进化主循环（替换原来的 MiningLoop）

```python
# src/mining/evolution.py

class EvolutionEngine:
    """
    因子进化引擎。不是随机搜索，而是知识驱动的进化。
    
    一代 = 一批因子（population_size 个）
    每代流程：
    1. 生成：从知识库假说 + 上一代的变异生成新因子
    2. 评估：全部回测
    3. 选择：保留 IC > 阈值的
    4. 分析：失败因子做原因分析
    5. 变异：失败因子的变异版本进入下一代
    6. 杂交：有效因子之间做交叉组合
    """
    
    def __init__(self, db, knowledge_base, llm_client):
        self.db = db
        self.kb = knowledge_base
        self.llm = llm_client
        self.analyzer = FailureAnalyzer()
        self.mutator = FactorMutator()
        self.registry = FactorRegistry()
        
    def run(self, generations: int = 10, population_size: int = 10):
        """
        运行进化。
        
        典型配置：
        - 10 代 × 10 个因子/代 = 100 次评估
        - 每次评估约 2000 tokens（构造代码）
        - 总计约 20 万 tokens ≈ $0.6
        - 耗时约 1-2 小时
        
        预期产出：
        - 100 次评估中，约 5-15 个通过验收
        - 其中 3-5 个是真正独立的新因子
        """
        
        # 第一代：从知识库所有 testable_predictions 生成
        population = self._generate_from_knowledge()
        
        all_accepted = []
        
        for gen in range(generations):
            print(f"\n{'='*60}")
            print(f"  Generation {gen+1}/{generations}")
            print(f"  Population: {len(population)} candidates")
            print(f"{'='*60}")
            
            results = []
            for i, candidate in enumerate(population):
                print(f"  [{i+1}/{len(population)}] Testing: {candidate['name']}")
                
                # 构造因子代码（LLM）
                code = self._construct_factor(candidate)
                if code is None:
                    continue
                    
                # 回测
                backtest = self._evaluate(code)
                results.append({
                    'candidate': candidate,
                    'code': code,
                    'backtest': backtest,
                })
                
                if backtest['accepted']:
                    print(f"    ✅ ACCEPTED: IC={backtest['ic_mean']:.4f} ICIR={backtest['icir']:.2f}")
                    all_accepted.append(candidate)
                else:
                    print(f"    ❌ REJECTED: {backtest.get('rejected_reason', 'unknown')}")
            
            # 分析失败并生成下一代
            next_population = []
            
            # 1. 失败因子的变异
            for r in results:
                if not r['backtest']['accepted']:
                    analysis = self.analyzer.analyze(r['candidate'], r['backtest'])
                    mutations = self.mutator.mutate(r['candidate'], analysis)
                    next_population.extend(mutations)
            
            # 2. 成功因子之间的杂交
            if len(all_accepted) >= 2:
                crosses = self._crossover(all_accepted)
                next_population.extend(crosses)
            
            # 3. 从知识库补充新假说（还没测试过的）
            untested = self._get_untested_predictions()
            next_population.extend(untested[:3])  # 每代最多补充 3 个新假说
            
            # 控制种群大小
            population = next_population[:population_size]
            
            # 打印代际摘要
            self._print_generation_summary(gen, results, all_accepted)
        
        return all_accepted
    
    def _generate_from_knowledge(self) -> list[dict]:
        """从知识库生成第一代候选因子。"""
        candidates = []
        for theory in self.kb.theories:
            for pred in theory['testable_predictions']:
                candidates.append({
                    'name': pred['id'],
                    'theory': theory['name'],
                    'hypothesis': pred['prediction'],
                    'factor_type': pred['factor_type'],
                    'conditions': pred.get('conditions', []),
                    'expression': pred.get('expression', ''),
                    'target': pred['target'],
                    'generation': 0,
                    'parent': None,
                    'mutation_type': 'seed',
                })
        return candidates
    
    def _construct_factor(self, candidate: dict) -> str | None:
        """用 LLM 将候选转化为可执行代码。
        
        关键：prompt 不是让 LLM 发明因子，
        而是让 LLM 实现一个已经定义好的因子。
        LLM 的角色是"工程师"而不是"研究员"。
        """
        prompt = f"""
你是一个量化因子工程师。请将以下因子假说实现为 Python 代码。

## 因子定义
名称：{candidate['name']}
理论来源：{candidate['theory']}
假说：{candidate['hypothesis']}
因子类型：{candidate['factor_type']}
{'条件：' + str(candidate['conditions']) if candidate['conditions'] else ''}
{'表达式：' + candidate['expression'] if candidate['expression'] else ''}
预测目标：{candidate['target']}

## 技术要求
- 继承 BaseFactor（公式因子）或 ConditionalFactor（条件因子）
- compute(universe, as_of, db) -> pd.Series
- 开头调用 validate_no_future()
- 只用 db.query() / db.query_range() 获取数据
- 处理空数据返回空 Series

## 可用数据表
daily_price: stock_code, trade_date, open, high, low, close, volume, amount, turnover_rate
zt_pool: stock_code, trade_date, consecutive_zt, amount, circulation_mv, open_count, zt_stats
zb_pool: stock_code, trade_date, amount, open_count
market_emotion: trade_date, zt_count, dt_count, highest_board, sentiment_level
fund_flow: stock_code, trade_date, super_large_net, large_net
concept_mapping: stock_code, concept_name
concept_daily: concept_name, trade_date, zt_count, leader_code, leader_consecutive
news: stock_code, title, publish_time, sentiment_score

直接输出 Python 代码，不要解释。
"""
        # 调用 Anthropic API
        pass
    
    def _crossover(self, accepted_factors: list) -> list[dict]:
        """有效因子之间做杂交。
        
        杂交方式：
        1. 乘法交叉：factor_a × factor_b
        2. 条件交叉：factor_a 为正时才用 factor_b
        3. 互补交叉：在 factor_a 无效的 regime 下用 factor_b
        """
        crosses = []
        for i, a in enumerate(accepted_factors):
            for b in accepted_factors[i+1:]:
                crosses.append({
                    'name': f"{a['name']}_x_{b['name']}",
                    'theory': f"交叉: {a['theory']} × {b['theory']}",
                    'hypothesis': f"在{a['hypothesis']}成立时，{b['hypothesis']}的信号更强",
                    'factor_type': 'cross',
                    'factor_a': a['name'],
                    'factor_b': b['name'],
                    'target': a['target'],
                    'generation': max(a.get('generation', 0), b.get('generation', 0)) + 1,
                    'parent': f"{a['name']}+{b['name']}",
                    'mutation_type': 'crossover',
                })
        return crosses[:5]  # 限制杂交数量
```

---

## 四、重新设计的 Prompt 策略

原设计的 Idea Agent prompt 让 LLM 做"研究员"（自己想假说），这是错的。
新设计让 LLM 做三种不同角色：

### 4.1 工程师角色（Factor Agent）— 最常用
```
"这是一个已定义好的因子假说，请实现为可执行代码。"
```
LLM 不需要创造力，只需要把结构化定义翻译为正确的 Python。
temperature = 0.1，最确定性。

### 4.2 分析师角色（失败分析增强）— 偶尔用
```
"以下因子回测 IC=0.02（不够），ICIR=0.3（不稳定）。
样本量每天平均 15 只。IC 在情绪强势时为 0.06，弱势时为 -0.01。
请分析失败原因并建议 3 个改进方向。"
```
LLM 协助分析师理解回测结果，但最终的变异操作由代码执行。
temperature = 0.5。

### 4.3 研究员角色（知识库耗尽后）— 很少用
```
"以下是当前所有已验证的因子和失败的假说。
已验证有效：[...]
已验证无效：[...]
请提出一个全新的、不与已有假说重叠的市场假说。
要求：必须有清晰的因果逻辑，不能是纯统计规律。"
```
只在知识库里所有 testable_predictions 都测完后才用。
temperature = 0.9。

---

## 五、CLI 命令

```bash
# 从知识库运行完整进化（推荐）
python -m cli.mine evolve --generations 10 --population 10

# 只测试知识库中的所有种子假说（不进化，快速验证）
python -m cli.mine test-seeds

# 对指定因子做变异探索
python -m cli.mine mutate --factor cascade_momentum --rounds 5

# 查看进化历史
python -m cli.mine history

# 查看因子家谱（哪个因子从哪个变异来的）
python -m cli.mine lineage --factor xxx
```

---

## 六、与原设计的对比

| 维度 | 原设计 | 新设计 |
|------|--------|--------|
| 假说来源 | LLM 随机猜 | 知识库推导 + LLM 补充 |
| 因子类型 | 只有公式因子 | 公式 + 条件组合 + 交叉 |
| 失败处理 | 扔掉重来 | 诊断原因 → 定向变异 → 重试 |
| LLM 角色 | 研究员（创造） | 工程师（实现） + 分析师（诊断） |
| 搜索策略 | 独立随机搜索 | 进化（保留好的基因，淘汰差的） |
| 你的认知 | 没有利用 | 知识库是核心驱动力 |
| 预期效率 | 100轮出1个有效因子 | 100轮出5-15个有效因子 |

---

## 七、执行顺序

```
Step A: 创建 knowledge_base/theories.yaml（从已有 SKILL.md 提取）
Step B: 实现 ConditionalFactor 和 CrossFactor 基类
Step C: 实现 FailureAnalyzer
Step D: 实现 FactorMutator
Step E: 实现 EvolutionEngine
Step F: 创建 CLI 命令
Step G: 先运行 test-seeds 验证所有种子假说
Step H: 运行完整进化 10 代
```
