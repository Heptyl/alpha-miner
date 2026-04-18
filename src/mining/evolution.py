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

        return candidates

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
                model="claude-sonnet-4-20250514",
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
        """无 LLM 时的模板代码生成。"""
        config = candidate.config
        factor_type = config.get("factor_type", "conditional")

        if factor_type == "conditional":
            conditions = config.get("conditions", [])
            cond_lines = []
            for c in conditions:
                if isinstance(c, dict):
                    cond_lines.append(f"    # {c}")
                else:
                    cond_lines.append(f"    # condition: {c}")
            conditions_str = "\n".join(cond_lines) if cond_lines else "    pass"
            return f'''"""Auto-generated factor: {candidate.name}"""
import pandas as pd
from src.data.storage import Storage

def compute(universe, as_of, db):
    """{config.get("prediction", "")}"""
{conditions_str}
    return pd.Series(dtype=float)
'''
        else:
            return f'''"""Auto-generated factor: {candidate.name}"""
import pandas as pd
from src.data.storage import Storage

def compute(universe, as_of, db):
    """{config.get("prediction", "")}"""
    # expression: {config.get("expression", "N/A")}
    return pd.Series(dtype=float)
'''

    # --------------------------------------------------
    # 沙箱评估
    # --------------------------------------------------

    def _evaluate(self, candidate: Candidate):
        """沙箱执行 + IC 回测。"""
        # 先构建代码
        if not candidate.code:
            candidate.code = self._construct_factor(candidate)

        if not candidate.code:
            candidate.error = "代码生成失败"
            return

        # 沙箱执行
        result = self.sandbox.execute(candidate.code, candidate.name)

        if result.get("error"):
            candidate.error = result["error"]
            return

        # IC 评估
        ic_result = result.get("ic_result", {})
        candidate.evaluation = ic_result

        # 验收判定
        ic = ic_result.get("ic_mean", 0.0)
        icir = ic_result.get("icir", 0.0)
        win_rate = ic_result.get("win_rate", 0.0)

        candidate.accepted = (
            abs(ic) >= self.MIN_IC
            and abs(icir) >= self.MIN_ICIR
            and win_rate >= self.MIN_WIN_RATE
        )

    # --------------------------------------------------
    # 变异
    # --------------------------------------------------

    def _mutate_accepted(self, candidate: Candidate) -> list[Candidate]:
        """对已验收因子做定向变异。"""
        if not candidate.evaluation:
            return []

        diagnosis = self.failure_analyzer.analyze(candidate.name, candidate.evaluation)
        mutations_config = self.mutator.mutate(candidate.config, {
            "diagnosis": diagnosis.diagnosis,
            "details": diagnosis.details,
        })

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
    # 杂交
    # --------------------------------------------------

    def _crossover(self, accepted: list[Candidate]) -> list[Candidate]:
        """有效因子杂交 — 取两个因子的条件组合。"""
        if len(accepted) < 2:
            return []

        results = []
        # 取最近两个验收因子杂交
        parents = accepted[-2:]
        p1_config = parents[0].config
        p2_config = parents[1].config

        hybrid_conditions = []
        # 父本1的前半条件 + 父本2的后半条件
        c1 = p1_config.get("conditions", [])
        c2 = p2_config.get("conditions", [])
        if c1 and c2:
            mid1 = max(1, len(c1) // 2)
            mid2 = max(1, len(c2) // 2)
            hybrid_conditions = c1[:mid1] + c2[mid2:]

        if hybrid_conditions:
            hybrid_name = f"{parents[0].name}_x_{parents[1].name}"
            results.append(Candidate(
                name=hybrid_name,
                source="crossover",
                config={
                    "name": hybrid_name,
                    "factor_type": "conditional",
                    "conditions": hybrid_conditions,
                    "parent1": parents[0].name,
                    "parent2": parents[1].name,
                },
            ))

        return results

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
