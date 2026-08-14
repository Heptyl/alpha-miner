"""跨平台每日流程编排 —— 替代 scripts/daily_run.sh，Windows / Linux 通用。

用法:
    python -m cli daily [--date YYYY-MM-DD]

每个交易日 15:40 后运行。各步骤以子进程串行执行（遇错即停，等价 bash 的 set -e），
并自动注入 PYTHONUTF8=1，规避 Windows 中文(GBK) locale 的编码问题。
"""

import argparse
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

# (标题, 传给 python 的参数)。{date} 会被替换为目标日期。
STEPS = [
    ("1/8 采集数据", ["-m", "cli.collect", "--today"]),
    ("2/8 计算因子值", ["-m", "cli.backtest", "--compute-today"]),
    ("2.5/8 Regime 识别", ["-c",
        "from src.data.storage import Storage; "
        "from src.pipeline.runner import run_regime_pipeline; "
        "db=Storage(); db.init_db(); run_regime_pipeline(db)"]),
    ("3/8 漂移检测", ["-m", "cli.drift", "--date", "{date}"]),
    ("4/8 因子进化", [
        "-m", "cli.mine", "evolve", "--generations", "3", "--population", "5",
        "--workers", "{workers}",
    ]),
    ("5/8 生成日报", ["-m", "cli.report", "--date", "{date}"]),
    ("6/8 生成市场剧本", ["-m", "cli", "script", "--date", "{date}", "--save"]),
    ("7/8 复盘昨日剧本", ["-m", "cli", "replay", "--date", "{date}", "--save"]),
    ("8/8 生成审视简报", ["scripts/generate_brief.py", "--date", "{date}"]),
]


def _run(title: str, args: list[str], date: str, workers: int) -> None:
    print(f"\n[{title}]", flush=True)
    cmd = [sys.executable] + [
        a.replace("{date}", date).replace("{workers}", str(workers))
        for a in args
    ]
    env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
    result = subprocess.run(cmd, cwd=str(ROOT), env=env)
    if result.returncode != 0:
        print(f"[ERROR] 步骤失败: {title} (exit {result.returncode})", flush=True)
        sys.exit(result.returncode)


def main() -> None:
    parser = argparse.ArgumentParser(description="Alpha Miner 每日流程(跨平台)")
    parser.add_argument("--date", default=datetime.now().strftime("%Y-%m-%d"),
                        help="目标日期 YYYY-MM-DD，默认今天")
    parser.add_argument(
        "--evolution-workers",
        type=int,
        default=int(os.environ.get("ALPHA_MINER_WORKERS", "1")),
        help="进化候选并行数；本机默认1，计算服务器建议8-16",
    )
    args = parser.parse_args()

    print(f"===== Alpha Miner Daily Run: {args.date} =====", flush=True)
    for title, step_args in STEPS:
        _run(title, step_args, args.date, args.evolution_workers)
    print(f"\n===== Done: {args.date} =====", flush=True)


if __name__ == "__main__":
    main()
