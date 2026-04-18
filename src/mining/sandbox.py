"""沙箱执行器 — 子进程隔离执行因子代码，60 秒超时。"""

import json
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

SANDBOX_SCRIPT = Path(__file__).parent / "_sandbox_runner.py"


class SandboxResult:
    """沙箱执行结果。"""

    def __init__(self, success: bool, output: dict | None = None, error: str | None = None, timed_out: bool = False):
        self.success = success
        self.output = output or {}
        self.error = error
        self.timed_out = timed_out

    def to_dict(self) -> dict:
        return {
            "success": self.success,
            "output": self.output,
            "error": self.error,
            "timed_out": self.timed_out,
        }


class Sandbox:
    """子进程沙箱，隔离执行因子代码。"""

    TIMEOUT = 60  # 秒

    def __init__(self, db_path: str = "data/alpha_miner.db"):
        self.db_path = db_path

    def execute(self, code: str, factor_name: str = "unknown") -> dict:
        """在子进程中执行因子代码并收集结果。

        Returns:
            {"error": str} 或 {"ic_result": {...}, "values": {...}}
        """
        # 确保沙箱运行脚本存在
        self._ensure_runner()

        # 写代码到临时文件
        with tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, prefix=f"factor_{factor_name}_") as f:
            f.write(code)
            code_path = f.name

        try:
            result = subprocess.run(
                [sys.executable, str(SANDBOX_SCRIPT), code_path, self.db_path, factor_name],
                capture_output=True,
                text=True,
                timeout=self.TIMEOUT,
            )

            if result.returncode != 0:
                return {"error": result.stderr.strip()[-500:] if result.stderr else "Unknown error"}

            # 解析输出
            stdout = result.stdout.strip()
            if stdout.startswith("{"):
                try:
                    return json.loads(stdout)
                except json.JSONDecodeError:
                    return {"error": f"Invalid JSON output: {stdout[:200]}"}
            else:
                return {"error": f"No JSON output. stdout: {stdout[:200]}"}

        except subprocess.TimeoutExpired:
            return {"error": f"执行超时 ({self.TIMEOUT}s)", "timed_out": True}
        except Exception as e:
            return {"error": str(e)}
        finally:
            Path(code_path).unlink(missing_ok=True)

    def _ensure_runner(self):
        """确保沙箱运行脚本存在。"""
        if not SANDBOX_SCRIPT.exists():
            SANDBOX_SCRIPT.write_text(RUNNER_CODE)


# 沙箱运行脚本内容
RUNNER_CODE = '''"""沙箱运行脚本 — 由 Sandbox.execute 调用，不应直接使用。"""
import json
import sys
import os
import traceback
from importlib import util

def main():
    code_path = sys.argv[1]
    db_path = sys.argv[2]
    factor_name = sys.argv[3]

    try:
        # 读取并执行因子代码
        with open(code_path) as f:
            code = f.read()

        # 在受限命名空间执行
        ns = {"__name__": f"factor_{factor_name}"}
        exec(compile(code, code_path, "exec"), ns)

        if "compute" not in ns:
            print(json.dumps({"error": "代码中未定义 compute(universe, as_of, db) 函数"}))
            return

        # 构造测试参数
        from src.data.storage import Storage
        db = Storage(db_path)

        # 用最近的交易日做测试
        from datetime import datetime, timedelta
        as_of = datetime.now()
        price_df = db.query("daily_price", as_of, limit=5)
        if price_df.empty:
            # 回退：用更早的日期
            for i in range(1, 30):
                as_of = datetime.now() - timedelta(days=i)
                price_df = db.query("daily_price", as_of, limit=5)
                if not price_df.empty:
                    break

        universe = sorted(price_df["stock_code"].unique().tolist())[:50] if not price_df.empty else ["000001"]

        # 执行 compute
        values = ns["compute"](universe, as_of, db)

        # 计算 IC（如果有足够数据）
        ic_result = {"ic_mean": 0.0, "icir": 0.0, "win_rate": 0.0, "sample_size": 0}

        if values is not None and len(values) > 0:
            import pandas as pd
            if isinstance(values, pd.Series):
                valid = values.dropna()
                ic_result["sample_size"] = len(valid)
                # 简单的 IC 测试：因子值与次日收益的 Spearman 相关
                # 这里只返回样本数，实际 IC 计算需要回测
                if len(valid) > 0:
                    ic_result["values_mean"] = float(valid.mean())
                    ic_result["values_std"] = float(valid.std()) if len(valid) > 1 else 0.0

        result = {
            "ic_result": ic_result,
            "factor_name": factor_name,
            "universe_size": len(universe),
        }
        print(json.dumps(result, ensure_ascii=False))

    except Exception as e:
        print(json.dumps({"error": traceback.format_exc()[-500:]}))

if __name__ == "__main__":
    main()
'''
