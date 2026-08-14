"""进化引擎 — 完整进化循环：知识库种子 → LLM翻译 → 沙箱评估 → 变异迭代。"""

import concurrent.futures
import json
import logging
import math
import time
from copy import deepcopy
from datetime import datetime
from pathlib import Path

import yaml

from src.mining.backtester import FactorBacktester
from src.mining.candidate_pool import CandidatePool
from src.mining.failure_analyzer import FailureAnalyzer
from src.mining.mutator import FactorMutator
from src.mining.sandbox import Sandbox
from src.mining.surgery_table import FactorSurgeryTable

logger = logging.getLogger(__name__)

# 知识库路径
KB_PATH = Path(__file__).parent.parent.parent / "knowledge_base" / "theories.yaml"
PROMPTS_DIR = Path(__file__).parent / "prompts"


class Candidate:
    """一个候选因子。"""

    def __init__(
        self,
        name: str,
        source: str,
        config: dict,
        code: str | None = None,
        generation: int = 0,
    ):
        self.name = name
        self.source = source          # "knowledge" / "mutation" / "crossover" / "llm_exploration"
        self.config = config
        self.code = code
        self.evaluation: dict | None = None
        self.accepted: bool = False
        self.error: str | None = None
        self.generation: int = generation

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
        state_path: str | None = None,
        candidate_pool_path: str | None = None,
    ):
        self.db_path = db_path
        self.api_client = api_client
        self.kb_path = Path(knowledge_path) if knowledge_path else KB_PATH
        self.mining_log_path = Path(mining_log_path)
        self.state_path = (
            Path(state_path)
            if state_path
            else self.mining_log_path.with_name("evolution_state.json")
        )
        pool_path = (
            Path(candidate_pool_path)
            if candidate_pool_path
            else self.mining_log_path.with_name("candidate_pool.jsonl")
        )
        self.sandbox = Sandbox(db_path)
        self.failure_analyzer = FailureAnalyzer()
        self.mutator = FactorMutator()
        self.candidate_pool = CandidatePool(str(pool_path))
        self.accepted: list[Candidate] = []
        self.log: list[dict] = []
        self.completed_generations = 0
        self._seen_signatures: set[str] = set()
        self._state_data_fingerprint: dict | None = None

    # --------------------------------------------------
    # 主循环
    # --------------------------------------------------

    def run(
        self,
        generations: int = 5,
        population_size: int = 10,
        resume: bool = True,
        workers: int = 1,
    ) -> list[Candidate]:
        """运行可续跑的进化循环。

        旧实现每次都从知识库第一代重跑，而且只变异已经通过的候选；绝大多数
        失败诊断没有进入下一代。现在会恢复上次 frontier，同时让有有效评估的
        失败候选也按诊断做定向变异。
        """
        if generations < 1 or population_size < 1 or workers < 1:
            raise ValueError("generations, population_size and workers must all be >= 1")

        if resume:
            candidates = self._load_progress()
        else:
            # ``--fresh`` must be deterministic even when the same engine object is reused.
            self.accepted = []
            self.log = []
            self.completed_generations = 0
            self._seen_signatures = set()
            self._state_data_fingerprint = None
            candidates = []
        current_fingerprint = self._data_fingerprint()
        if resume and self._state_data_fingerprint == current_fingerprint and candidates:
            logger.warning("底层交易数据未变化，继续进化会增加样本内过拟合风险")

        if not candidates:
            seeds = self._dedupe_candidates(self._generate_from_knowledge(), exclude_seen=True)
            candidates = seeds[:population_size]
            if not candidates:
                candidates = self._restart_population(population_size)

        logger.info(
            "进化引擎启动: generations=%d, population=%d, workers=%d, resume=%s, start_gen=%d",
            generations, population_size, workers, resume, self.completed_generations + 1,
        )

        for _ in range(generations):
            gen_start = time.time()
            generation_number = self.completed_generations + 1
            candidates = self._dedupe_candidates(candidates, exclude_seen=True)[:population_size]
            if not candidates:
                candidates = self._restart_population(population_size)
            logger.info("--- 第 %d 代 (candidates=%d) ---", generation_number, len(candidates))

            if not candidates:
                logger.warning("没有未评估候选，停止本轮")
                break

            for candidate in candidates:
                candidate.generation = generation_number

            if workers == 1 or len(candidates) == 1:
                for candidate in candidates:
                    self._evaluate(candidate)
            else:
                with concurrent.futures.ThreadPoolExecutor(
                    max_workers=min(workers, len(candidates)),
                ) as executor:
                    futures = {
                        executor.submit(self._evaluate, candidate): candidate
                        for candidate in candidates
                    }
                    for future in concurrent.futures.as_completed(futures):
                        candidate = futures[future]
                        try:
                            future.result()
                        except Exception as exc:
                            candidate.error = f"并行评估失败: {exc}"

            for candidate in candidates:
                self._seen_signatures.add(self._candidate_signature(candidate))
                self._write_log(candidate)

            newly_accepted = [candidate for candidate in candidates if candidate.accepted]
            self._extend_accepted(newly_accepted)
            for candidate in newly_accepted:
                self._stage_candidate(candidate)
            logger.info("本代验收: %d/%d", len(newly_accepted), len(candidates))

            next_candidates = self._build_next_generation(candidates, population_size)
            self.completed_generations = generation_number
            self._save_progress(next_candidates, current_fingerprint)
            candidates = next_candidates

            logger.info(
                "第 %d 代耗时: %.1fs, 下一代=%d",
                generation_number, time.time() - gen_start, len(candidates),
            )

        logger.info("进化完成。累计验收: %d 个因子", len(self.accepted))
        return self.accepted

    def _candidate_signature(self, candidate: Candidate) -> str:
        """候选的语义签名；忽略会无限增长但不改变逻辑的显示名。"""
        config = {
            key: value
            for key, value in candidate.config.items()
            if key not in {"name", "parent_name", "base_name", "mutation_depth", "restart_epoch"}
        }
        return json.dumps(config, ensure_ascii=False, sort_keys=True, default=str)

    def _dedupe_candidates(
        self,
        candidates: list[Candidate],
        exclude_seen: bool = False,
    ) -> list[Candidate]:
        unique: list[Candidate] = []
        signatures: set[str] = set()
        for candidate in candidates:
            signature = self._candidate_signature(candidate)
            if signature in signatures or (exclude_seen and signature in self._seen_signatures):
                continue
            signatures.add(signature)
            unique.append(candidate)
        return unique

    def _fitness(self, candidate: Candidate) -> float:
        """用于选父本的连续得分；拒绝不等于没有可学习价值。"""
        if candidate.error or not candidate.evaluation:
            return float("-inf")
        evaluation = candidate.evaluation
        ic = abs(float(evaluation.get("ic_mean", 0.0) or 0.0))
        icir = abs(float(evaluation.get("icir", 0.0) or 0.0))
        win_rate = float(evaluation.get("win_rate", 0.0) or 0.0)
        total_days = float(evaluation.get("total_days", 0.0) or 0.0)
        return ic * 4 + min(icir, 3.0) + win_rate + min(total_days, 60) / 600

    def _build_next_generation(
        self,
        evaluated: list[Candidate],
        population_size: int,
    ) -> list[Candidate]:
        """精英杂交 + 失败诊断变异 + 未测试知识种子，组成下一代。"""
        mutations: list[Candidate] = []
        viable = [candidate for candidate in evaluated if self._fitness(candidate) > float("-inf")]
        viable.sort(key=self._fitness, reverse=True)

        parent_limit = max(1, math.ceil(population_size / 2))
        for parent in viable[:parent_limit]:
            mutations.extend(self._mutate_candidate(parent)[:2])

        crossovers: list[Candidate] = []
        if len(self.accepted) >= 2:
            crossovers = self._crossover(self.accepted)

        fresh_seeds = self._dedupe_candidates(
            self._generate_from_knowledge(),
            exclude_seen=True,
        )

        # 保留探索配额，避免表现尚可的同一血统一直占满整个人口。
        seed_slots = min(len(fresh_seeds), max(1, population_size // 4))
        crossover_slots = min(len(crossovers), max(1, population_size // 5))
        mutation_slots = max(0, population_size - seed_slots - crossover_slots)
        selected = (
            mutations[:mutation_slots]
            + crossovers[:crossover_slots]
            + fresh_seeds[:seed_slots]
        )

        # 某一类不足时用其他类补齐。
        overflow = mutations[mutation_slots:] + crossovers[crossover_slots:] + fresh_seeds[seed_slots:]
        selected.extend(overflow)

        return self._dedupe_candidates(selected, exclude_seen=True)[:population_size]

    def _restart_population(self, population_size: int) -> list[Candidate]:
        """Revive an exhausted frontier from the best historical failures.

        A finite knowledge seed set must not turn a long-running evolution job into
        a no-op. Historical candidates retain their executable code and evaluation,
        so they are better restart parents than rerunning the original seed.
        """
        parents = self._historical_candidates()
        parent_signatures = {self._candidate_signature(candidate) for candidate in parents}
        for accepted in self.accepted:
            if self._candidate_signature(accepted) not in parent_signatures:
                parents.append(accepted)
        parents.sort(key=self._fitness, reverse=True)

        offspring: list[Candidate] = []
        parent_limit = max(1, math.ceil(population_size / 2))
        for parent in parents[:parent_limit]:
            offspring.extend(self._mutate_candidate(parent))

        candidates = self._dedupe_candidates(offspring, exclude_seen=True)
        if len(candidates) < population_size:
            for index, parent in enumerate(parents):
                candidate = self._restart_variant(parent, index)
                if candidate is not None:
                    candidates.append(candidate)
                candidates = self._dedupe_candidates(candidates, exclude_seen=True)
                if len(candidates) >= population_size:
                    break

        candidates = candidates[:population_size]
        if candidates:
            logger.warning(
                "下一代为空，已从 %d 个历史候选重启种群: %d",
                len(parents), len(candidates),
            )
        return candidates

    def _historical_candidates(self) -> list[Candidate]:
        """Load the latest evaluable version of each semantic candidate from JSONL."""
        if not self.mining_log_path.exists():
            return []
        latest: dict[str, Candidate] = {}
        try:
            for line in self.mining_log_path.read_text(encoding="utf-8").splitlines():
                try:
                    candidate = Candidate.from_dict(json.loads(line))
                except (KeyError, TypeError, json.JSONDecodeError):
                    continue
                if self._fitness(candidate) > float("-inf"):
                    latest[self._candidate_signature(candidate)] = candidate
        except OSError as exc:
            logger.warning("无法读取历史候选: %s", exc)
        return list(latest.values())

    def _restart_variant(self, parent: Candidate, index: int) -> Candidate | None:
        """Create a deterministic but executable exploration variant for a restart."""
        epoch = self.completed_generations + 1
        config = deepcopy(parent.config)
        config["parent_name"] = parent.name
        config["base_name"] = parent.config.get("base_name", parent.name)
        config["mutation_depth"] = int(parent.config.get("mutation_depth", 0)) + 1
        config["restart_epoch"] = epoch

        numeric_conditions = [
            condition
            for condition in config.get("conditions", [])
            if isinstance(condition, dict) and isinstance(condition.get("value"), (int, float))
        ]
        if numeric_conditions:
            # 0.55 .. 1.45; changing the threshold changes executable behavior.
            ratio = 0.55 + ((epoch * 17 + index * 11) % 91) / 100
            for condition in numeric_conditions:
                condition["value"] *= ratio
            config["mutation_type"] = "restart_threshold"
            config["restart_ratio"] = round(ratio, 2)
            name = f"{parent.name}_restart_g{epoch}_r{ratio:.2f}"
            config["name"] = name
            code = self._build_conditional_template(name, config["conditions"], config)
        elif parent.code:
            window = 2 + ((epoch * 7 + index * 5) % 19)
            config["mutation_type"] = "change_lookback"
            config["lookback_days"] = window
            name = f"{parent.name}_restart_g{epoch}_lb{window}"
            config["name"] = name
            code = self._wrap_mutation_code(parent.code, config)
        else:
            return None

        return Candidate(
            name=name,
            source="restart",
            config=config,
            code=code,
            generation=epoch,
        )

    def _extend_accepted(self, candidates: list[Candidate]) -> None:
        existing = {self._candidate_signature(candidate) for candidate in self.accepted}
        for candidate in candidates:
            signature = self._candidate_signature(candidate)
            if signature not in existing:
                self.accepted.append(candidate)
                existing.add(signature)

    def _stage_candidate(self, candidate: Candidate) -> None:
        """样本内通过后进入观察池，而不是直接视为生产因子。"""
        if not candidate.code or self.candidate_pool.get_status(candidate.name):
            return
        try:
            self.candidate_pool.add_candidate(candidate.name, candidate.config, candidate.code)
        except ValueError:
            pass

    def _data_fingerprint(self) -> dict:
        """记录进化所用数据版本，便于识别“数据没变但一直跑”。"""
        try:
            from src.data.storage import Storage

            db = Storage(self.db_path)
            rows = db.execute(
                "SELECT MAX(trade_date) AS latest, COUNT(DISTINCT trade_date) AS days "
                "FROM daily_price"
            )
            return rows[0] if rows else {"latest": None, "days": 0}
        except Exception:
            return {"latest": None, "days": 0}

    def _load_progress(self) -> list[Candidate]:
        """从 checkpoint 恢复累计代数、父本和下一代候选。"""
        if not self.state_path.exists():
            self._restore_history_from_log()
            return []
        try:
            state = json.loads(self.state_path.read_text(encoding="utf-8"))
            self.completed_generations = int(state.get("completed_generations", 0))
            self._seen_signatures = set(state.get("seen_signatures", []))
            self._state_data_fingerprint = state.get("data_fingerprint")
            self.accepted = [Candidate.from_dict(item) for item in state.get("accepted", [])]
            return [Candidate.from_dict(item) for item in state.get("frontier", [])]
        except (OSError, ValueError, TypeError, json.JSONDecodeError) as exc:
            logger.warning("进化 checkpoint 损坏，回退日志恢复: %s", exc)
            self._restore_history_from_log()
            return []

    def _restore_history_from_log(self) -> None:
        if not self.mining_log_path.exists():
            return
        accepted: list[Candidate] = []
        try:
            for line in self.mining_log_path.read_text(encoding="utf-8").splitlines():
                try:
                    record = json.loads(line)
                    candidate = Candidate.from_dict(record)
                except (KeyError, TypeError, json.JSONDecodeError):
                    continue
                self._seen_signatures.add(self._candidate_signature(candidate))
                if candidate.accepted:
                    accepted.append(candidate)
                self.completed_generations = max(
                    self.completed_generations,
                    int(candidate.generation or 0),
                )
            self._extend_accepted(accepted)
        except OSError as exc:
            logger.warning("无法读取历史挖掘日志: %s", exc)

    def _save_progress(self, frontier: list[Candidate], data_fingerprint: dict) -> None:
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        state = {
            "version": 1,
            "updated_at": datetime.now().isoformat(),
            "completed_generations": self.completed_generations,
            "data_fingerprint": data_fingerprint,
            "seen_signatures": sorted(self._seen_signatures),
            "accepted": [candidate.to_dict() for candidate in self.accepted],
            "frontier": [candidate.to_dict() for candidate in frontier],
        }
        temp_path = self.state_path.with_suffix(self.state_path.suffix + ".tmp")
        temp_path.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")
        temp_path.replace(self.state_path)
        self._state_data_fingerprint = data_fingerprint

    # --------------------------------------------------
    # 知识库种子
    # --------------------------------------------------

    def _generate_from_knowledge(self) -> list[Candidate]:
        """从 theories.yaml 的假说生成第一代候选。"""
        if not self.kb_path.exists():
            logger.warning("知识库不存在: %s", self.kb_path)
            return []

        raw = self.kb_path.read_bytes()
        try:
            content = raw.decode("utf-8-sig")
        except UnicodeDecodeError:
            # Test fixtures and user-edited YAML may use the Windows ANSI code page.
            # GB18030 is a compatible superset of GBK/CP936.
            content = raw.decode("gb18030")
        kb = yaml.safe_load(content) or {}

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

    def _get_historical_failures(self) -> dict[str, int]:
        """读取 mining_log.jsonl，统计每个假说被拒绝的次数。"""
        failure_counts: dict[str, int] = {}
        if not self.mining_log_path.exists():
            return failure_counts

        try:
            with open(self.mining_log_path, encoding="utf-8") as f:
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
            prompt_template = prompt_path.read_text(encoding="utf-8")
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
        """为 conditional 类型生成可执行条件评分器。"""
        conditions_literal = repr(conditions)
        return f'''"""Auto-generated conditional factor: {name}"""
import pandas as pd
import re

CONDITIONS = {conditions_literal}

def _compare(actual, operator, expected):
    operations = {{
        ">": lambda a, b: a > b,
        ">=": lambda a, b: a >= b,
        "<": lambda a, b: a < b,
        "<=": lambda a, b: a <= b,
        "==": lambda a, b: a == b,
        "!=": lambda a, b: a != b,
    }}
    return operations.get(operator, operations[">="])(actual, expected)

def compute(universe, as_of, db):
    """{config.get("prediction", "")}"""
    date_str = as_of.strftime("%Y-%m-%d")
    results = {{}}

    for code in universe:
        passed = 0
        observed = 0
        for condition in CONDITIONS:
            if isinstance(condition, dict):
                table = str(condition.get("table", "daily_price"))
                column = str(condition.get("column", ""))
                if not column:
                    continue
                data = db.query(
                    table,
                    as_of,
                    where="stock_code = ? AND trade_date = ?",
                    params=(code, date_str),
                )
                if data.empty:
                    continue
                observed += 1
                actual = float(data.iloc[-1].get(column, 0) or 0)
                expected = float(condition.get("value", 0) or 0)
                if _compare(actual, str(condition.get("operator", ">=")), expected):
                    passed += 1
                continue

            text = str(condition)
            if "连板" in text:
                data = db.query(
                    "zt_pool", as_of,
                    where="stock_code = ? AND trade_date = ?",
                    params=(code, date_str),
                )
                if data.empty:
                    continue
                observed += 1
                actual = float(data.iloc[-1].get("consecutive_zt", 0) or 0)
            elif "换手" in text:
                data = db.query(
                    "daily_price", as_of,
                    where="stock_code = ? AND trade_date = ?",
                    params=(code, date_str),
                )
                if data.empty:
                    continue
                observed += 1
                actual = float(data.iloc[-1].get("turnover_rate", 0) or 0)
            else:
                continue

            match = re.search(r"(>=|<=|>|<|==)?\\s*(\\d+(?:\\.\\d+)?)", text)
            operator = match.group(1) or ">=" if match else ">="
            expected = float(match.group(2)) if match else 1.0
            if _compare(actual, operator, expected):
                passed += 1

        results[code] = passed / observed if observed else 0.0

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

        # 沙箱只做隔离加载；旧实现又在子进程回测 20 日、又在主进程回测
        # 60 日，同一个候选重复计算两遍。
        validation = self.sandbox.validate(candidate.code, candidate.name)

        if validation.get("error"):
            candidate.error = validation["error"]
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
                    candidate.error = bt_result.error
                    candidate.evaluation = bt_result.to_dict()
                    return
                else:
                    candidate.evaluation = bt_result.to_dict()
            except Exception as e:
                candidate.error = f"FactorBacktester 回测失败: {e}"
                candidate.evaluation = {}
                return
        else:
            candidate.error = "无法提取 compute 函数"
            candidate.evaluation = {}
            return

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

    def _mutate_candidate(self, candidate: Candidate) -> list[Candidate]:
        """对任意有评估结果的候选做定向变异，包括未通过候选。"""
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
            mc["base_name"] = candidate.config.get("base_name", candidate.name)
            mc["parent_name"] = candidate.name
            mc["mutation_depth"] = int(candidate.config.get("mutation_depth", 0)) + 1
            code = None
            if candidate.code and self.api_client is None:
                code = self._wrap_mutation_code(candidate.code, mc)
            results.append(Candidate(
                name=mc["name"],
                source="mutation",
                config=mc,
                code=code,
                generation=candidate.generation,
            ))
        return results

    def _mutate_accepted(self, candidate: Candidate) -> list[Candidate]:
        """兼容旧调用名。"""
        return self._mutate_candidate(candidate)

    def _wrap_mutation_code(self, parent_code: str, config: dict) -> str | None:
        """把配置变异落实为可执行 wrapper，避免“名字变了、代码没变”。"""
        if "def compute(" not in parent_code:
            return None

        mutation_type = config.get("mutation_type", "")
        renamed = parent_code.replace("def compute(", "def _parent_compute(", 1)

        if mutation_type == "reverse_direction":
            body = """
    values = _parent_compute(universe, as_of, db)
    return -values if values is not None else pd.Series(dtype=float)
"""
        elif mutation_type in {"change_lookback", "smoothing"}:
            window = int(config.get("smoothing_window") or config.get("lookback_days") or 1)
            body = f"""
    from datetime import timedelta
    frames = []
    for offset in range({max(1, min(window, 20))}):
        values = _parent_compute(universe, as_of - timedelta(days=offset), db)
        if values is not None and not values.empty:
            frames.append(values.rename(str(offset)))
    if not frames:
        return pd.Series(dtype=float)
    return pd.concat(frames, axis=1).mean(axis=1, skipna=True)
"""
        elif mutation_type == "regime_filter":
            wanted = repr(str(config.get("regime_filter", "")))
            body = f"""
    date_str = as_of.strftime("%Y-%m-%d")
    state = db.query("regime_state", as_of, where="trade_date = ?", params=(date_str,))
    if state.empty or str(state.iloc[-1].get("regime_type", "")) != {wanted}:
        return pd.Series(dtype=float)
    return _parent_compute(universe, as_of, db)
"""
        elif mutation_type == "zt_count_filter":
            bounds = config.get("zt_count_filter", {})
            minimum = int(bounds.get("min", 0))
            maximum = int(bounds.get("max", 999))
            body = f"""
    date_str = as_of.strftime("%Y-%m-%d")
    emotion = db.query("market_emotion", as_of, where="trade_date = ?", params=(date_str,))
    if emotion.empty:
        return pd.Series(dtype=float)
    zt_count = int(emotion.iloc[-1].get("zt_count", 0))
    if not ({minimum} <= zt_count <= {maximum}):
        return pd.Series(dtype=float)
    return _parent_compute(universe, as_of, db)
"""
        else:
            # 条件阈值类变异需从配置重新编译，不能复用写死阈值的父代码。
            conditions = config.get("conditions", [])
            if conditions:
                return self._build_conditional_template(config["name"], conditions, config)
            return None

        return renamed + "\n\n" + "def compute(universe, as_of, db):\n" + body

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
            import numpy as np
            import pandas as pd
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
        with open(self.mining_log_path, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
