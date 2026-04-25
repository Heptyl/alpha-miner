"""挖掘 CLI — python -m cli.mine

用法:
  python -m cli.mine evolve --generations 10 --population 10
  python -m cli.mine test-seeds
  python -m cli.mine mutate --factor cascade_momentum --rounds 5
  python -m cli.mine history
  python -m cli.mine lineage --factor xxx
  python -m cli.mine surgery --factor consecutive_board --days 60
"""

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import yaml

from src.data.storage import Storage
from src.factors.registry import FactorRegistry
from src.mining.backtester import FactorBacktester
from src.mining.evolution import EvolutionEngine, Candidate
from src.mining.failure_analyzer import FailureAnalyzer
from src.mining.mutator import FactorMutator
from src.mining.surgery_table import FactorSurgeryTable


def _build_llm_client():
    """构建 LLM client，优先 Z.AI Anthropic 兼容接口。"""
    import os

    # 1) 环境变量
    api_key = os.environ.get("GLM_API_KEY") or os.environ.get("ZAI_API_KEY")
    if api_key:
        try:
            import anthropic
            return anthropic.Anthropic(
                api_key=api_key,
                base_url="https://open.bigmodel.cn/api/anthropic",
            ), "Z.AI (env)"
        except ImportError:
            pass

    # 2) openclaw.json
    try:
        import json as _json
        oc = _json.load(open(os.path.expanduser("~/.openclaw/openclaw.json")))
        api_key = oc.get("env", {}).get("ZAI_API_KEY")
        if api_key:
            import anthropic
            return anthropic.Anthropic(
                api_key=api_key,
                base_url="https://open.bigmodel.cn/api/anthropic",
            ), "Z.AI (openclaw.json)"
    except Exception:
        pass

    # 3) hermes auth.json
    try:
        import json as _json
        auth = _json.load(open(os.path.expanduser("~/.hermes/auth.json")))
        pool = auth.get("credential_pool", {}).get("zai", [])
        if pool:
            cred = pool[0]
            api_key = cred.get("access_token")
            base_url = cred.get("base_url", "https://open.bigmodel.cn/api/anthropic")
            if api_key:
                import anthropic
                return anthropic.Anthropic(
                    api_key=api_key,
                    base_url=base_url,
                ), f"Z.AI ({cred.get('label', 'auth.json')})"
    except Exception:
        pass

    return None, None


def cmd_evolve(args):
    """完整进化循环。"""
    api_client, source = _build_llm_client()
    if api_client:
        print(f"[INFO] LLM 已连接: {source}")
    else:
        print("[INFO] 无 LLM 可用，使用模板生成")

    engine = EvolutionEngine(
        db_path=args.db,
        api_client=api_client,
        mining_log_path=args.log,
    )

    print(f"\n{'='*60}")
    print(f"  Alpha Miner 进化引擎")
    print(f"  generations={args.generations}, population={args.population}")
    print(f"{'='*60}\n")

    accepted = engine.run(generations=args.generations, population_size=args.population)

    print(f"\n{'='*60}")
    print(f"  进化完成")
    print(f"  总验收因子: {len(accepted)}")
    print(f"{'='*60}")

    for c in accepted:
        ic = c.evaluation.get("ic_mean", 0) if c.evaluation else 0
        print(f"  {c.name:<30} IC={ic:.4f}  gen={c.generation}  source={c.source}")


def cmd_test_seeds(args):
    """只测试知识库种子，不进化。"""
    engine = EvolutionEngine(db_path=args.db, mining_log_path=args.log)
    candidates = engine._generate_from_knowledge()

    if not candidates:
        print("[ERROR] 知识库为空或不存在")
        return

    print(f"[INFO] 测试 {len(candidates)} 个种子假说\n")
    print(f"  {'假说ID':<30} {'状态':<10} {'IC':>8} {'样本':>8}")
    print("  " + "-" * 60)

    for c in candidates:
        c.generation = 0
        engine._evaluate(c)

        if c.error:
            status = "ERROR"
            ic_str = "N/A"
            sample_str = "N/A"
        elif c.accepted:
            status = "ACCEPTED"
            ic_str = f"{c.evaluation.get('ic_mean', 0):.4f}"
            sample_str = str(c.evaluation.get('sample_size', 0))
        else:
            status = "REJECTED"
            ic_str = f"{c.evaluation.get('ic_mean', 0):.4f}" if c.evaluation else "N/A"
            sample_str = str(c.evaluation.get('sample_size', 0)) if c.evaluation else "N/A"

        print(f"  {c.name:<30} {status:<10} {ic_str:>8} {sample_str:>8}")

    accepted = [c for c in candidates if c.accepted]
    print(f"\n  验收: {len(accepted)}/{len(candidates)}")


def cmd_mutate(args):
    """对指定因子做变异探索。"""
    mutator = FactorMutator()
    analyzer = FailureAnalyzer()

    # 构造原始因子配置
    config = {
        "name": args.factor,
        "factor_type": "conditional",
        "conditions": [],
        "lookback_days": 5,
    }

    # 如果有回测结果，读入
    result = {"ic_mean": 0.0, "icir": 0.0, "avg_sample_per_day": 0, "max_correlation": 0.0}

    print(f"[INFO] 对 {args.factor} 做 {args.rounds} 轮变异\n")

    all_variants = [config]
    current = config

    for r in range(args.rounds):
        diagnosis = analyzer.analyze(args.factor, result)
        mutations = mutator.mutate(current, {"diagnosis": diagnosis.diagnosis, "details": diagnosis.details})
        print(f"  Round {r+1}: {diagnosis.diagnosis} → {len(mutations)} 个变异")
        for m in mutations:
            print(f"    - {m['name']}")
        all_variants.extend(mutations)
        if mutations:
            current = mutations[0]

    print(f"\n  总计 {len(all_variants)} 个变体（含原始）")


def cmd_history(args):
    """查看历史挖掘记录。"""
    log_path = Path(args.log)
    if not log_path.exists():
        print("[INFO] 无挖掘记录")
        return

    lines = log_path.read_text().strip().split("\n")
    if not lines or lines[0] == "":
        print("[INFO] 无挖掘记录")
        return

    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    print(f"[INFO] 共 {len(records)} 条挖掘记录\n")
    print(f"  {'时间':<22} {'因子':<25} {'来源':<12} {'状态':<10} {'IC':>8}")
    print("  " + "-" * 80)

    for r in records[-50:]:  # 最近50条
        ts = r.get("timestamp", "")[:19]
        name = r.get("name", "")[:24]
        source = r.get("source", "")
        accepted = "ACCEPTED" if r.get("accepted") else "REJECTED"
        ic = r.get("evaluation", {}).get("ic_mean", 0)
        ic_str = f"{ic:.4f}" if ic else "N/A"
        print(f"  {ts:<22} {name:<25} {source:<12} {accepted:<10} {ic_str:>8}")


def cmd_lineage(args):
    """查看因子家谱。"""
    log_path = Path(args.log)
    if not log_path.exists():
        print("[INFO] 无挖掘记录")
        return

    lines = log_path.read_text().strip().split("\n")
    records = []
    for line in lines:
        try:
            records.append(json.loads(line))
        except json.JSONDecodeError:
            continue

    # 查找相关记录
    target = args.factor
    related = [r for r in records if target in r.get("name", "") or target in r.get("config", {}).get("parent1", "") or target in r.get("config", {}).get("parent2", "")]

    if not related:
        print(f"[INFO] 未找到因子 {target} 的记录")
        return

    print(f"[INFO] 因子 {target} 家谱 ({len(related)} 条记录)\n")
    for r in related:
        config = r.get("config", {})
        source = r.get("source", "")
        gen = r.get("generation", 0)
        ic = r.get("evaluation", {}).get("ic_mean", 0)
        accepted = "✓" if r.get("accepted") else "✗"

        parents = ""
        if config.get("parent1") or config.get("parent2"):
            parents = f" ← {config.get('parent1', '?')} × {config.get('parent2', '?')}"

        print(f"  Gen {gen} | {accepted} | {source:<12} | IC={ic:.4f} | {r.get('name', '')}{parents}")


def cmd_surgery(args):
    """因子手术台 — 解剖因子IC序列，诊断有效性来源。"""
    factor_name = args.factor
    lookback_days = args.days
    ic_threshold = args.threshold

    # 1. 创建 Storage 和 Backtester
    db = Storage(db_path=args.db)
    backtester = FactorBacktester(db=db)

    # 2. 从因子注册表查找因子
    registry = FactorRegistry()
    try:
        factor = registry.get_factor(factor_name)
    except KeyError as e:
        print(f"[ERROR] {e}")
        print(f"[INFO] 可用因子: {', '.join(registry.list_factors())}")
        sys.exit(1)

    compute_fn = factor.compute

    # 3. 运行回测
    print(f"[INFO] 因子手术台: {factor_name}")
    print(f"[INFO] 回测窗口: {lookback_days} 天, IC阈值: {ic_threshold}")
    print()

    result = backtester.run(compute_fn, factor_name, lookback_days=lookback_days)

    # 4. 检查回测错误
    if result.error:
        print(f"[ERROR] 回测失败: {result.error}")
        sys.exit(1)

    # 5. 运行手术台分析
    surgery = FactorSurgeryTable()
    report = surgery.analyze(result.ic_series, factor_name, ic_threshold)

    # 6. 打印报告
    print(f"{'='*60}")
    print(f"  因子手术台报告: {report.factor_name}")
    print(f"{'='*60}\n")

    # 整体 IC / ICIR
    print(f"  整体 IC:   {report.overall_ic:+.4f}")
    print(f"  整体 ICIR: {report.overall_icir:+.4f}")
    print()

    # Regime 分段
    if report.regime_breakdown:
        print(f"  {'='*55}")
        print(f"  市场状态分段 (Regime Breakdown)")
        print(f"  {'='*55}")
        print(f"  {'Regime':<16} {'IC均值':>8} {'ICIR':>8} {'天数':>6} {'有效':>6}")
        print(f"  {'-'*50}")
        for r in report.regime_breakdown:
            eff = "✓" if r.effective else "✗"
            print(f"  {r.regime:<16} {r.ic_mean:>+8.4f} {r.icir:>+8.4f} {r.sample_days:>6} {eff:>6}")
        print()

    # 情绪分段
    if report.emotion_breakdown:
        print(f"  {'='*55}")
        print(f"  情绪分段 (Emotion Breakdown)")
        print(f"  {'='*55}")
        print(f"  {'情绪桶':<16} {'IC均值':>8} {'ICIR':>8} {'天数':>6} {'有效':>6}")
        print(f"  {'-'*50}")
        bucket_labels = {"strong": "强势", "normal": "正常", "weak": "弱势"}
        for e in report.emotion_breakdown:
            eff = "✓" if e.effective else "✗"
            label = bucket_labels.get(e.bucket, e.bucket)
            print(f"  {label:<16} {e.ic_mean:>+8.4f} {e.icir:>+8.4f} {e.sample_days:>6} {eff:>6}")
        print()

    # 时间分段对比
    if report.time_decay:
        print(f"  {'='*55}")
        print(f"  时间分段对比 (Time Decay)")
        print(f"  {'='*55}")
        print(f"  {'时间段':<16} {'IC均值':>8} {'天数':>6}")
        print(f"  {'-'*35}")
        seg_labels = {"first_half": "前半段", "second_half": "后半段"}
        for t in report.time_decay:
            label = seg_labels.get(t.segment, t.segment)
            print(f"  {label:<16} {t.ic_mean:>+8.4f} {t.sample_days:>6}")
        print()

    # 黄金窗口
    if report.golden_windows:
        print(f"  {'='*55}")
        print(f"  黄金窗口 (Golden Windows)")
        print(f"  {'='*55}")
        print(f"  {'起始日':<14} {'结束日':<14} {'Regime':<14} {'平均IC':>8}")
        print(f"  {'-'*55}")
        for w in report.golden_windows:
            print(f"  {w.start_date:<14} {w.end_date:<14} {w.regime:<14} {w.avg_ic:>+8.4f}")
        print()

    # 诊断 + 建议
    diagnosis_labels = {
        "universally_effective": "全局有效",
        "regime_dependent": "依赖市场状态",
        "emotion_dependent": "依赖情绪环境",
        "time_decayed": "时间衰减",
        "no_signal": "无信号",
    }
    diag_label = diagnosis_labels.get(report.diagnosis, report.diagnosis)
    print(f"  {'='*55}")
    print(f"  诊断结果")
    print(f"  {'='*55}")
    print(f"  类型: {diag_label}")
    if report.best_regime:
        print(f"  最佳市场状态: {report.best_regime}")
    if report.best_emotion:
        emotion_label = bucket_labels.get(report.best_emotion, report.best_emotion)
        print(f"  最佳情绪环境: {emotion_label}")
    print(f"  建议: {report.suggestion}")
    print()


def main():
    parser = argparse.ArgumentParser(description="Alpha Miner 挖掘工具")
    parser.add_argument("--db", type=str, default="data/alpha_miner.db", help="数据库路径")
    parser.add_argument("--log", type=str, default="data/mining_log.jsonl", help="挖掘日志路径")
    subparsers = parser.add_subparsers(dest="command")

    # evolve
    p_evolve = subparsers.add_parser("evolve", help="完整进化循环")
    p_evolve.add_argument("--generations", type=int, default=5, help="进化代数")
    p_evolve.add_argument("--population", type=int, default=10, help="每代种群大小")

    # test-seeds
    subparsers.add_parser("test-seeds", help="测试知识库种子假说")

    # mutate
    p_mutate = subparsers.add_parser("mutate", help="对指定因子做变异探索")
    p_mutate.add_argument("--factor", type=str, required=True, help="因子名")
    p_mutate.add_argument("--rounds", type=int, default=5, help="变异轮数")

    # history
    subparsers.add_parser("history", help="查看历史挖掘记录")

    # lineage
    p_lineage = subparsers.add_parser("lineage", help="查看因子家谱")
    p_lineage.add_argument("--factor", type=str, required=True, help="因子名")

    # surgery
    p_surgery = subparsers.add_parser("surgery", help="因子手术台 — 解剖IC序列，诊断有效性来源")
    p_surgery.add_argument("--factor", type=str, required=True, help="因子名")
    p_surgery.add_argument("--days", type=int, default=60, help="回测窗口天数 (默认60)")
    p_surgery.add_argument("--threshold", type=float, default=0.03, help="IC诊断阈值 (默认0.03)")

    args = parser.parse_args()

    if args.command == "evolve":
        cmd_evolve(args)
    elif args.command == "test-seeds":
        cmd_test_seeds(args)
    elif args.command == "mutate":
        cmd_mutate(args)
    elif args.command == "history":
        cmd_history(args)
    elif args.command == "lineage":
        cmd_lineage(args)
    elif args.command == "surgery":
        cmd_surgery(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
