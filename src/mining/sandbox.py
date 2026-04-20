"""沙箱执行器 — 子进程隔离执行因子代码，60 秒超时。"""

import json
import subprocess
import sys
import tempfile
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

    TIMEOUT = 120  # 秒（IC 回测需要更长时间）

    def __init__(self, db_path: str = "data/alpha_miner.db"):
        self.db_path = db_path

    def execute(self, code: str, factor_name: str = "unknown") -> dict:
        """在子进程中执行因子代码并收集结果。

        Returns:
            {"error": str} 或 {"ic_result": {...}, ...}
        """
        if not SANDBOX_SCRIPT.exists():
            return {"error": f"沙箱脚本不存在: {SANDBOX_SCRIPT}"}

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
