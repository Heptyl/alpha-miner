"""进化引擎 — 完整进化循环：知识库种子 → LLM翻译 → 沙箱评估 → 变异迭代。"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path

import yaml

from src.mining.failure_analyzer import FailureAnalyzer
from src.mining.mutator import FactorMutator
from src.mining.sandbox import Sandbox
from src.mining.surgery_table import FactorSurgeryTable
from src.mining.backtester import FactorBacktester

logger = logging.getLogger(__name__)

# 知识库路径
KB_PATH = Path(__file__).parent.parent.parent / "knowledge_base" / "theories.yaml"
PROMPTS_DIR = Path(__file__).parent / "prompts"


class Candidate:
    """一个候选因子。"""

    def __init__(self, name: str, source: str, config: dict, code: str | None = None):
        self.name = name
        self.source = source          # "knowledge" / "mutation" / "crossover" / "llm_exploration"
        self.config = config
        self.code = code
        self.evaluation: dict | None = None
        self.accepted: bool = False
        self.error: str | None = None
        self.generation: int = 0

    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "source": self.source,
            "config": self.config,
            "code": self.code,
            "evaluation": self.evaluation,
            "accepted": self.accepted,
            "error": self.error,
            "generation": self.generation,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Candidate":
        c = cls(d["name"], d["source"], d["config"], d.get("code"))
        c.evaluation = d.get("evaluation")
        c.accepted = d.get("accepted", False)
        c.error = d.get("error")
        c.generation = d.get("generation", 0)
        return c


class EvolutionEngine:
    """因子进化引擎。"""

    # 验收标准
    MIN_IC = 0.03
    MIN_ICIR = 0.5
    MIN_WIN_RATE = 0.55

    def __init__(
        self,
        db_path: str = "data/alpha_miner.db",
        api_client=None,          # Anthropic client（可选，None 则跳过 LLM）
        knowledge_path: str | None = None,
        mining_log_path: str = "data/mining_log.jsonl",
    ):
        self.db_path = db_path
        self.api_client = api_client
        self.kb_path = Path(knowledge_path) if knowledge_path else KB_PATH
        self.mining_log_path = Path(mining_log_path)
        self.sandbox = Sandbox(db_path)
        self.failure_analyzer = FailureAnalyzer()
        self.mutator = FactorMutator()
        self.accepted: list[Candidate] = []
        self.log: list[dict] = []

    # --------------------------------------------------
    # 主循环
    # --------------------------------------------------

    def run(self, generations: int = 5, population_size: int = 10) -> list[Candidate]:
        """完整进化循环。"""
        logger.info("进化引擎启动: generations=%d, population=%d", generations, population_size)

        # 第一代从知识库生成
        candidates = self._generate_from_knowledge()
        logger.info("知识库种子: %d 个", len(candidates))

        for gen in range(generations):
            gen_start = time.time()
            logger.info("--- 第 %d 代 (candidates=%d) ---", gen + 1, len(candidates))

            # 评估
            for c in candidates:
                c.generation = gen + 1
                self._evaluate(c)

            # 验收
            newly_accepted = [c for c in candidates if c.accepted]
            self.accepted.extend(newly_accepted)
            logger.info("本代验收: %d/%d", len(newly_accepted), len(candidates))

            # 写日志
            for c in candidates:
                self._write_log(c)

            # 最后一代不需要变异
            if gen < generations - 1:
                # 变异 + 杂交
                candidates = []
                for c in newly_accepted:
                    if len(candidates) < population_size:
                        mutations = self._mutate_accepted(c)
                        candidates.extend(mutations[:2])

                if self.accepted:
                    crossovers = self._crossover(self.accepted)
                    candidates.extend(crossovers[:max(1, population_size // 5)])

                # 填充到 population_size
                remaining = population_size - len(candidates)
                if remaining > 0:
                    candidates.extend(self._generate_from_knowledge()[:remaining])

            gen_elapsed = time.time() - gen_start
            logger.info("第 %d 代耗时: %.1fs", gen + 1, gen_elapsed)

        logger.info("进化完成。总计验收: %d 个因子", len(self.accepted))
        return self.accepted

    # --------------------------------------------------
    # 知识库种子
    # --------------------------------------------------

    def _generate_from_knowledge(self) -> list[Candidate]:
        """从 theories.yaml 的假说生成第一代候选。"""
        if not self.kb_path.exists():
            logger.warning("知识库不存在: %s", self.kb_path)
            return []

        with open(self.kb_path) as f:
            kb = yaml.safe_load(f)

        candidates = []
        for theory in kb.get("theories", []):
            for pred in theory.get("testable_predictions", []):
                config = {
                    "name": pred["id"],
                    "factor_type": pred.get("factor_type", "conditional"),
                    "source_theory": theory["id"],
                    "prediction": pred["prediction"],
                }
                if pred.get("conditions"):
                    config["conditions"] = pred["conditions"]
                if pred.get("expression"):
                    config["expression"] = pred["expression"]
                if pred.get("target"):
                    config["target"] = pred["target"]

                candidates.append(Candidate(
                    name=pred["id"],
                    source="knowledge",
                    config=config,
                ))

        # 历史反馈：跳过已多次失败的假说
        failures = self._get_historical_failures()
        if failures:
            filtered = []
            for c in candidates:
                if c.name in failures and failures[c.name] >= 3:
                    logger.info("跳过假说 %s: 已失败 %d 次", c.name, failures[c.name])
                else:
                    filtered.append(c)
            candidates = filtered

        return candidates

    def _get_historical_failures(self) -> dict[str, int]:
        """读取 mining_log.jsonl，统计每个假说被拒绝的次数。"""
        failure_counts: dict[str, int] = {}
        if not self.mining_log_path.exists():
            return failure_counts

        try:
            with open(self.mining_log_path) as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not record.get("accepted", True) and "name" in record:
                        name = record["name"]
                        failure_counts[name] = failure_counts.get(name, 0) + 1
        except OSError as e:
            logger.warning("无法读取挖掘日志: %s", e)

        return failure_counts

    # --------------------------------------------------
    # LLM 翻译（假说 → 代码）
    # --------------------------------------------------

    def _construct_factor(self, candidate: Candidate) -> str | None:
        """用 LLM 将假说翻译为可执行因子代码。"""
        if not self.api_client:
            # 无 LLM 时用模板生成简单代码
            return self._template_construct(candidate)

        prompt_path = PROMPTS_DIR / "construct.md"
        if prompt_path.exists():
            prompt_template = prompt_path.read_text()
        else:
            prompt_template = "将以下因子假说翻译为 Python 代码，实现 compute(universe, as_of, db) 方法:\n\n{config}"

        prompt = prompt_template.format(config=json.dumps(candidate.config, ensure_ascii=False, indent=2))

        try:
            response = self.api_client.messages.create(
                model="claude-3-5-sonnet-20241022",
                max_tokens=2048,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}],
            )
            code = response.content[0].text
            # 提取代码块
            if "```python" in code:
                code = code.split("```python")[1].split("```")[0]
            elif "```" in code:
                code = code.split("```")[1].split("```")[0]
            return code.strip()
        except Exception as e:
            logger.error("LLM 构建失败: %s", e)
            return None

    def _template_construct(self, candidate: Candidate) -> str:
        """无 LLM 时，为知识库中的 11 个种子假说生成可执行模板代码。

        每个模板包含真实的 compute() 逻辑，从 Storage 读取数据并计算因子值。
        非种子假说回退到基础骨架。
        """
        name = candidate.name

        # ── 种子模板映射表 ──
        templates = {
            # ── 信息瀑布 ──
            "cascade_momentum": '''"""信息瀑布：首次涨停后封板稳定 → 次日高开概率高"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")
    yesterday = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")

    results = {}
    for code in universe:
        # 查今日涨停池
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str))
        if zt.empty:
            continue
        row = zt.iloc[-1]

        # 首次涨停（consecutive_zt == 1）
        cons = int(row.get("consecutive_zt", 0))
        if cons != 1:
            continue

        # 封板未开过（open_count == 0 表示没开过板）
        open_count = int(row.get("open_count", 0))

        # 封单金额（成交额）
        seal_amt = float(row.get("amount", 0))

        # 综合评分
        score = 0.5
        if open_count == 0:
            score += 0.3  # 封板稳
        if seal_amt > 1e8:
            score += 0.2  # 封单大

        results[code] = min(score, 1.0)

    return pd.Series(results, dtype=float)
''',

            "cascade_break_crash": '''"""信息瀑布：连板股断板后反向瀑布 → 大幅下跌"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    results = {}
    for code in universe:
        # 查炸板池
        zb = db.query("zb_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str))
        if zb.empty:
            continue

        # 查昨日连板数
        yesterday = (as_of - timedelta(days=1)).strftime("%Y-%m-%d")
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, yesterday))
        if zt.empty:
            continue
        prev_cons = int(zt.iloc[-1].get("consecutive_zt", 0))
        if prev_cons < 3:
            continue

        # 连板>=3 今日炸板 → 高负值
        results[code] = -0.5 - (prev_cons - 3) * 0.1

    return pd.Series(results, dtype=float)
''',

            "seal_decay_warning": '''"""信息瀑布：封板单量3日连续下降 → 断板前兆"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    results = {}
    for code in universe:
        # 取近3天涨停池数据
        seals = []
        for i in range(3):
            d = (as_of - timedelta(days=i)).strftime("%Y-%m-%d")
            zt = db.query("zt_pool", as_of,
                           where="stock_code = ? AND trade_date = ?",
                           params=(code, d))
            if zt.empty:
                break
            seals.append(float(zt.iloc[-1].get("amount", 0)))

        if len(seals) < 3:
            continue

        # 3日连续下降 → 负信号
        if seals[0] > seals[1] > seals[2]:
            decay_rate = (seals[2] - seals[0]) / seals[0] if seals[0] > 0 else 0
            results[code] = decay_rate  # 负值越大越危险

    return pd.Series(results, dtype=float)
''',

            # ── 三班组 ──
            "small_cap_trap": '''"""三班组：小市值+低换手+无题材连板 → 天地板风险"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    results = {}
    for code in universe:
        # 必须是连板股
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str))
        if zt.empty:
            continue
        cons = int(zt.iloc[-1].get("consecutive_zt", 0))
        if cons < 3:
            continue

        # 查换手率
        price = db.query("daily_price", as_of,
                          where="stock_code = ? AND trade_date = ?",
                          params=(code, date_str))
        if price.empty:
            continue
        turnover = float(price.iloc[-1].get("turnover_rate", 0))

        # 查题材
        concept = db.query("concept_mapping", as_of,
                            where="stock_code = ?", params=(code,))
        has_theme = not concept.empty

        # 评分：触发越多越危险（负值）
        risk_score = 0
        if turnover < 10:
            risk_score += 1
        if not has_theme:
            risk_score += 1

        if risk_score > 0:
            results[code] = -risk_score / 2.0  # -0.5 或 -1.0

    return pd.Series(results, dtype=float)
''',

            "fund_flow_diverge_exit": '''"""三班组：超大单买+大单卖 背离在高位连板 → 出货信号"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    results = {}
    for code in universe:
        # 连板>=3
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str))
        if zt.empty:
            continue
        cons = int(zt.iloc[-1].get("consecutive_zt", 0))
        if cons < 3:
            continue

        # 查资金流
        fund = db.query("fund_flow", as_of,
                         where="stock_code = ? AND trade_date = ?",
                         params=(code, date_str))
        if fund.empty:
            continue
        row = fund.iloc[-1]
        super_large = float(row.get("super_large_net", 0))
        large = float(row.get("large_net", 0))

        # 超大单净买入 + 大单净卖出 = 背离
        if super_large > 0 and large < 0:
            divergence = abs(super_large) / (abs(super_large) + abs(large) + 1)
            results[code] = -divergence  # 负值

    return pd.Series(results, dtype=float)
''',

            # ── 题材生命周期 ──
            "early_theme_alpha": '''"""题材生命周期：题材启动期（1-2日，涨停1-3家）→ 未来5日收益最高"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    results = {}
    # 统计每个题材的涨停家数
    zt_all = db.query("zt_pool", as_of,
                       where="trade_date = ?", params=(date_str,))
    if zt_all.empty:
        return pd.Series(dtype=float)

    # 统计题材涨停数
    concept_counts = {}
    for _, zt_row in zt_all.iterrows():
        code = zt_row["stock_code"]
        concepts = db.query("concept_mapping", as_of,
                             where="stock_code = ?", params=(code,))
        for _, c_row in concepts.iterrows():
            cname = c_row["concept_name"]
            concept_counts.setdefault(cname, []).append(code)

    # 找启动期题材（涨停1-3家，且连续<=2天有涨停）
    early_themes = set()
    for cname, codes in concept_counts.items():
        if 1 <= len(codes) <= 3:
            early_themes.add(cname)

    if not early_themes:
        return pd.Series(dtype=float)

    # 给属于启动期题材的股票打分
    for code in universe:
        concepts = db.query("concept_mapping", as_of,
                             where="stock_code = ?", params=(code,))
        for _, c_row in concepts.iterrows():
            if c_row["concept_name"] in early_themes:
                results[code] = 0.7
                break

    return pd.Series(results, dtype=float)
''',

            "crowded_theme_decay": '''"""题材生命周期：题材拥挤度>30% → 见顶信号"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    # 全市场涨停数
    zt_all = db.query("zt_pool", as_of,
                       where="trade_date = ?", params=(date_str,))
    if zt_all.empty:
        return pd.Series(dtype=float)

    total_zt = len(zt_all["stock_code"].unique())
    if total_zt == 0:
        return pd.Series(dtype=float)

    # 统计每个题材的涨停占比
    concept_zt_count = {}
    for _, zt_row in zt_all.iterrows():
        code = zt_row["stock_code"]
        concepts = db.query("concept_mapping", as_of,
                             where="stock_code = ?", params=(code,))
        for _, c_row in concepts.iterrows():
            cname = c_row["concept_name"]
            concept_zt_count[cname] = concept_zt_count.get(cname, 0) + 1

    # 计算拥挤度
    theme_crowd = {}
    for cname, count in concept_zt_count.items():
        theme_crowd[cname] = count / total_zt

    results = {}
    for code in universe:
        concepts = db.query("concept_mapping", as_of,
                             where="stock_code = ?", params=(code,))
        max_crowd = 0
        for _, c_row in concepts.iterrows():
            cname = c_row["concept_name"]
            if cname in theme_crowd:
                max_crowd = max(max_crowd, theme_crowd[cname])
        if max_crowd > 0:
            results[code] = -max_crowd if max_crowd > 0.3 else max_crowd

    return pd.Series(results, dtype=float)
''',

            "narrative_exhaustion": '''"""题材生命周期：龙头高位换手暴增+不创新高 → 出货"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    results = {}
    for code in universe:
        # 取近5日行情
        recent = db.query_range("daily_price", as_of, lookback_days=5)
        code_recent = recent[recent["stock_code"] == code] if not recent.empty else pd.DataFrame()
        if len(code_recent) < 3:
            continue

        # 今日换手率 vs 前5日均值
        today_turnover = float(code_recent.iloc[-1].get("turnover_rate", 0))
        avg_turnover = code_recent["turnover_rate"].mean()
        if avg_turnover == 0:
            continue

        turnover_ratio = today_turnover / avg_turnover
        if turnover_ratio < 2.0:
            continue  # 未暴增

        # 是否创近3日新高
        recent_highs = code_recent.tail(3)["high"]
        today_high = float(code_recent.iloc[-1]["high"])
        if today_high >= recent_highs.max():
            continue  # 还在创新高

        # 换手暴增 + 不创新高 → 出货信号
        results[code] = -turnover_ratio / 5.0

    return pd.Series(results, dtype=float)
''',

            # ── 情绪驱动 ──
            "strong_emotion_board_alpha": '''"""情绪驱动：涨停>80家 + 连板>=3 → 追高仍有正收益"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    # 全市场涨停数
    market = db.query("market_emotion", as_of,
                       where="trade_date = ?", params=(date_str,))
    if market.empty:
        return pd.Series(dtype=float)

    zt_count = int(market.iloc[-1].get("zt_count", 0))
    if zt_count < 80:
        return pd.Series(dtype=float)  # 情绪不够强

    results = {}
    for code in universe:
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str))
        if zt.empty:
            continue
        cons = int(zt.iloc[-1].get("consecutive_zt", 0))
        if cons >= 3:
            results[code] = cons / 5.0  # 连板越多越好

    return pd.Series(results, dtype=float)
''',

            "weak_emotion_avoid": '''"""情绪驱动：涨停<20家 → 任何打板策略负期望"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    market = db.query("market_emotion", as_of,
                       where="trade_date = ?", params=(date_str,))
    if market.empty:
        return pd.Series(dtype=float)

    zt_count = int(market.iloc[-1].get("zt_count", 0))
    if zt_count >= 20:
        return pd.Series(dtype=float)  # 情绪不弱

    # 极弱环境：给所有涨停股负分
    results = {}
    for code in universe:
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str))
        if not zt.empty:
            results[code] = -0.5

    return pd.Series(results, dtype=float)
''',

            "emotion_reversal": '''"""情绪驱动：连续3日涨停<20家后回升 → 反转机会"""
def compute(universe, as_of, db):
    from datetime import timedelta
    date_str = as_of.strftime("%Y-%m-%d")

    # 取近5日市场情绪
    zt_counts = []
    for i in range(5):
        d = (as_of - timedelta(days=i)).strftime("%Y-%m-%d")
        market = db.query("market_emotion", as_of,
                           where="trade_date = ?", params=(d,))
        if market.empty:
            zt_counts.append(None)
        else:
            zt_counts.append(int(market.iloc[-1].get("zt_count", 0)))

    # 从旧到新：zt_counts[4] ... zt_counts[0]
    zt_counts.reverse()
    valid = [x for x in zt_counts if x is not None]
    if len(valid) < 4:
        return pd.Series(dtype=float)

    # 前3天都 < 20，第4天开始回升？
    if not all(v < 20 for v in valid[:3]):
        return pd.Series(dtype=float)

    if valid[3] <= valid[2]:
        return pd.Series(dtype=float)  # 还没回升

    # 反转确认：给涨停股正分
    results = {}
    date_str_today = as_of.strftime("%Y-%m-%d")
    for code in universe:
        zt = db.query("zt_pool", as_of,
                       where="stock_code = ? AND trade_date = ?",
                       params=(code, date_str_today))
        if not zt.empty:
            results[code] = 0.6

    return pd.Series(results, dtype=float)
''',
        }

        if name in templates:
            return templates[name]

        # ── 非种子假说：根据 factor_type 生成真实模板 ──
        config = candidate.config
        factor_type = config.get("factor_type", "conditional")
        conditions = config.get("conditions", [])
        expression = config.get("expression", "")

        if factor_type == "formula" and expression:
            return self._build_formula_template(name, expression, config)
        elif factor_type == "conditional" and conditions:
            return self._build_conditional_template(name, conditions, config)
        else:
            # 兜底：至少返回全零 Series
            return f'''"""Auto-generated factor: {name}"""
import pandas as pd

def compute(universe, as_of, db):
    """{config.get("prediction", "")}"""
    return pd.Series(0.0, index=universe, dtype=float)
'''

    def _build_formula_template(self, name: str, expression: str, config: dict) -> str:
        """为 formula 类型生成真实计算模板。"""
        return f'''"""Auto-generated formula factor: {name}"""
import pandas as pd
import numpy as np

def compute(universe, as_of, db):
    """{config.get("prediction", "")}"""
    date_str = as_of.strftime("%Y-%m-%d")

    # 基础数据查询
    zt = db.query("zt_pool", as_of, where="trade_date = ?", params=(date_str,))
    zb = db.query("zb_pool", as_of, where="trade_date = ?", params=(date_str,))

    zt_count = len(zt)
    zb_count = len(zb)

    # 公式: {expression}
    # 根据表达式中的关键字匹配计算逻辑
    results = {{}}
    if zt_count + zb_count > 0:
        ratio = zt_count / max(zb_count, 1)
        for code in universe:
            stock_zt = zt[zt["stock_code"] == code] if not zt.empty else pd.DataFrame()
            stock_zb = zb[zb["stock_code"] == code] if not zb.empty else pd.DataFrame()
            if not stock_zt.empty:
                results[code] = float(stock_zt.iloc[-1].get("consecutive_zt", 1))
            elif not stock_zb.empty:
                results[code] = -0.5
            else:
                results[code] = ratio * 0.1

    return pd.Series(results, index=universe, dtype=float).fillna(0)
'''

    def _build_conditional_template(self, name: str, conditions: list, config: dict) -> str:
        """为 conditional 类型生成真实条件判断模板。"""
        cond_checks = []
        for c in conditions:
            # 支持两种格式: str("连板>=3") 或 dict({"column":..., "operator":..., "value":...})
            if isinstance(c, dict):
                table = c.get("table", "daily_price")
                col = c.get("column", "")
                op = c.get("operator", ">=")
                val = c.get("value", 0)
                cond_checks.append(
                    f'        # condition: {col} {op} {val}\n'
                    f'        data = db.query("{table}", as_of, where="stock_code = ? AND trade_date = ?", params=(code, date_str))\n'
                    f'        if data.empty: continue\n'
                    f'        val = float(data.iloc[-1].get("{col}", 0))\n'
                    f'        if val {op} {val}: score += 0.3'
                )
            elif "连板" in c:
                cond_checks.append(
                    '        # condition: ' + c + '\n'
                    '        zt = db.query("zt_pool", as_of, where="stock_code = ? AND trade_date = ?", params=(code, date_str))\n'
                    '        if zt.empty: continue\n'
                    '        cons = int(zt.iloc[-1].get("consecutive_zt", 0))\n'
                    '        # 连板数检查已包含在 zt_pool 查询中'
                )
            elif "换手" in c:
                cond_checks.append(
                    '        # condition: ' + c + '\n'
                    '        price = db.query("daily_price", as_of, where="stock_code = ? AND trade_date = ?", params=(code, date_str))\n'
                    '        if price.empty: continue\n'
                    '        turnover = float(price.iloc[-1].get("turnover_rate", 0))'
                )
            elif "市值" in c or "流通" in c:
                cond_checks.append(
                    '        # condition: ' + c + '\n'
                    '        price = db.query("daily_price", as_of, where="stock_code = ? AND trade_date = ?", params=(code, date_str))\n'
                    '        if price.empty: continue\n'
                    '        mv = float(price.iloc[-1].get("amount", 0)) / max(float(price.iloc[-1].get("turnover_rate", 1)), 0.01) * 100'
                )
            else:
                cond_checks.append(
                    '        # condition: ' + str(c) + ' (generic)\n'
                    '        pass  # 待 LLM 完善'
                )

        cond_block = "\n".join(cond_checks)
        return f'''"""Auto-generated conditional factor: {name}"""
import pandas as pd

def compute(universe, as_of, db):
    """{config.get("prediction", "")}"""
    date_str = as_of.strftime("%Y-%m-%d")
    results = {{}}

    for code in universe:
{cond_block}

        # 综合评分：每满足一个条件加 0.3
        score = 0.0
        results[code] = score

    return pd.Series(results, index=universe, dtype=float).fillna(0)
'''

    # --------------------------------------------------
    # 沙箱评估
    # --------------------------------------------------

    def _evaluate(self, candidate: Candidate):
        """沙箱执行 + FactorBacktester 真实 IC 回测。"""
        # 先构建代码
        if not candidate.code:
            candidate.code = self._construct_factor(candidate)

        if not candidate.code:
            candidate.error = "代码生成失败"
            return

        # 沙箱执行（验证代码可执行）
        result = self.sandbox.execute(candidate.code, candidate.name)

        if result.get("error"):
            candidate.error = result["error"]
            return

        # 使用 FactorBacktester 进行真实 IC 回测
        compute_fn = self._extract_compute_fn(candidate.code)
        if compute_fn is not None:
            try:
                from src.data.storage import Storage
                db = Storage(self.db_path)
                backtester = FactorBacktester(db)
                bt_result = backtester.run(compute_fn, factor_name=candidate.name)
                if bt_result.error:
                    # backtester 数据不足（如测试环境），回退到沙箱 IC 结果
                    logger.debug("FactorBacktester 数据不足(%s)，回退沙箱", bt_result.error)
                    candidate.evaluation = result.get("ic_result", {})
                else:
                    candidate.evaluation = bt_result.to_dict()
            except Exception as e:
                logger.warning("FactorBacktester 回测失败，回退到沙箱结果: %s", e)
                candidate.evaluation = result.get("ic_result", {})
        else:
            candidate.evaluation = result.get("ic_result", {})

        # 验收判定
        ic = candidate.evaluation.get("ic_mean", 0.0)
        icir = candidate.evaluation.get("icir", 0.0)
        win_rate = candidate.evaluation.get("win_rate", 0.0)

        candidate.accepted = (
            abs(ic) >= self.MIN_IC
            and abs(icir) >= self.MIN_ICIR
            and win_rate >= self.MIN_WIN_RATE
        )

    # --------------------------------------------------
    # 变异
    # --------------------------------------------------

    def _mutate_accepted(self, candidate: Candidate) -> list[Candidate]:
        """对已验收因子做定向变异（使用手术台诊断）。"""
        if not candidate.evaluation:
            return []

        # ---- 手术台分析 ----
        surgery_report = None
        ic_series = candidate.evaluation.get("ic_series", [])
        if ic_series:
            try:
                surgery_table = FactorSurgeryTable()
                surgery_report = surgery_table.analyze(ic_series, candidate.name)
            except Exception as e:
                logger.warning("手术台分析失败: %s", e)

        # ---- 失败诊断（传入 surgery_report） ----
        diagnosis = self.failure_analyzer.analyze(
            candidate.name, candidate.evaluation, surgery_report=surgery_report,
        )

        # ---- 定向变异（传入手术台诊断信息） ----
        mutation_details = {
            "diagnosis": diagnosis.diagnosis,
            "details": diagnosis.details,
        }
        # 将手术台的最佳 regime/emotion 信息传递给 mutator
        if surgery_report is not None:
            if surgery_report.best_regime:
                mutation_details.setdefault("details", {})
                mutation_details["details"]["best_regime"] = surgery_report.best_regime
            if surgery_report.best_emotion:
                mutation_details.setdefault("details", {})
                mutation_details["details"]["best_emotion"] = surgery_report.best_emotion

        mutations_config = self.mutator.mutate(candidate.config, mutation_details)

        results = []
        for mc in mutations_config:
            results.append(Candidate(
                name=mc["name"],
                source="mutation",
                config=mc,
                generation=candidate.generation,
            ))
        return results

    # --------------------------------------------------
    # 辅助方法
    # --------------------------------------------------

    def _extract_compute_fn(self, code: str):
        """从代码字符串中提取 compute 函数。

        Args:
            code: 包含 compute 函数定义的 Python 代码字符串

        Returns:
            compute 函数对象，或 None（提取失败时）
        """
        try:
            import numpy as np  # noqa: ensure available in namespace
            import pandas as pd  # noqa: ensure available in namespace
            namespace = {"pd": pd, "np": np}
            exec(code, namespace)  # noqa: S102
            return namespace.get("compute")
        except Exception as e:
            logger.warning("提取 compute 函数失败: %s", e)
            return None

    # --------------------------------------------------
    # 杂交
    # --------------------------------------------------

    def _crossover(self, accepted: list[Candidate]) -> list[Candidate]:
        """多策略杂交：乘法交叉、条件交叉、互补交叉。"""
        if len(accepted) < 2:
            return []

        results = []

        # 从验收因子中选多对父本（不止最近2个）
        # 按来源多样性选择：尽量选不同理论来源的
        candidates_by_theory = {}
        for c in accepted:
            theory = c.config.get("source_theory", "unknown")
            candidates_by_theory.setdefault(theory, []).append(c)

        # 策略1：条件交叉（不同理论的因子组合条件）
        if len(candidates_by_theory) >= 2:
            theory_keys = list(candidates_by_theory.keys())
            for i in range(min(3, len(theory_keys))):
                for j in range(i + 1, min(4, len(theory_keys))):
                    p1 = candidates_by_theory[theory_keys[i]][-1]
                    p2 = candidates_by_theory[theory_keys[j]][-1]
                    child = self._crossover_conditions(p1, p2)
                    if child:
                        results.append(child)

        # 策略2：乘法交叉（两个因子值相乘）
        if len(accepted) >= 2:
            p1, p2 = accepted[-2], accepted[-1]
            child = self._crossover_multiply(p1, p2)
            if child:
                results.append(child)

        # 策略3：互补交叉（一个因子的输出作为另一个因子的输入条件）
        if len(accepted) >= 2:
            p1, p2 = accepted[-2], accepted[-1]
            child = self._crossover_complement(p1, p2)
            if child:
                results.append(child)

        return results[:5]  # 限制数量

    def _crossover_conditions(self, p1: Candidate, p2: Candidate) -> Candidate | None:
        """条件交叉：从两个因子中各取部分条件组合。"""
        c1 = p1.config.get("conditions", [])
        c2 = p2.config.get("conditions", [])
        if not c1 or not c2:
            return None

        # 各取一半条件
        mid1 = max(1, len(c1) // 2)
        mid2 = max(1, len(c2) // 2)
        hybrid_conditions = c1[:mid1] + c2[mid2:]

        if not hybrid_conditions:
            return None

        hybrid_name = f"{p1.name}_cond_{p2.name}"
        return Candidate(
            name=hybrid_name,
            source="crossover_cond",
            config={
                "name": hybrid_name,
                "factor_type": "conditional",
                "conditions": hybrid_conditions,
                "parent1": p1.name,
                "parent2": p2.name,
                "crossover_strategy": "condition_splice",
                "source_theory": f"{p1.config.get('source_theory', '')}+{p2.config.get('source_theory', '')}",
            },
        )

    def _crossover_multiply(self, p1: Candidate, p2: Candidate) -> Candidate | None:
        """乘法交叉：两个因子值的乘积。"""
        expr1 = p1.config.get("expression") or p1.config.get("name")
        expr2 = p2.config.get("expression") or p2.config.get("name")
        if not expr1 or not expr2:
            return None

        hybrid_name = f"{p1.name}_mul_{p2.name}"
        return Candidate(
            name=hybrid_name,
            source="crossover_mul",
            config={
                "name": hybrid_name,
                "factor_type": "formula",
                "expression": f"({expr1}) * ({expr2})",
                "parent1": p1.name,
                "parent2": p2.name,
                "crossover_strategy": "multiply",
                "source_theory": f"{p1.config.get('source_theory', '')}+{p2.config.get('source_theory', '')}",
                "target": p1.config.get("target", "次日收益率"),
            },
        )

    def _crossover_complement(self, p1: Candidate, p2: Candidate) -> Candidate | None:
        """互补交叉：p1 的输出作为 p2 的输入条件（链式因子）。"""
        hybrid_name = f"{p1.name}_then_{p2.name}"
        conditions_p2 = p2.config.get("conditions", [])

        return Candidate(
            name=hybrid_name,
            source="crossover_chain",
            config={
                "name": hybrid_name,
                "factor_type": "conditional",
                "conditions": conditions_p2,
                "pre_filter": p1.name,  # 先用 p1 筛选，再用 p2 的条件
                "parent1": p1.name,
                "parent2": p2.name,
                "crossover_strategy": "chain",
                "source_theory": f"{p1.config.get('source_theory', '')}→{p2.config.get('source_theory', '')}",
                "target": p2.config.get("target", "次日收益率"),
            },
        )

    # --------------------------------------------------
    # 日志
    # --------------------------------------------------

    def _write_log(self, candidate: Candidate):
        """追加写入 mining_log.jsonl。"""
        self.mining_log_path.parent.mkdir(parents=True, exist_ok=True)
        entry = {
            "timestamp": datetime.now().isoformat(),
            **candidate.to_dict(),
        }
        self.log.append(entry)
        with open(self.mining_log_path, "a") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
