"""Alpha Miner 主控制台 — 本地 Web UI，免手动敲命令。

纯标准库实现（http.server + subprocess + threading），不引入任何新依赖；
命令一律以当前解释器（.venv 的 python）作为子进程串行执行，同一时刻只跑一个任务。

用法:
    uv run python scripts/dashboard.py [--port 8765] [--open]
    或双击项目根目录 dashboard.bat

安全边界：只绑定 127.0.0.1；可执行命令固定在 COMMANDS 白名单内，
参数只允许日期(YYYY-MM-DD)与映射表内的因子名，不存在任意命令注入面。
"""

from __future__ import annotations

import argparse
import html
import json
import os
import re
import sqlite3
import subprocess
import sys
import threading
import webbrowser
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.factors.naming import get_naming  # noqa: E402

DB_PATH = ROOT / "data" / "alpha_miner.db"
BRIEF_LATEST = ROOT / "reports" / "brief" / "latest.html"

DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
MAX_LINES = 5000

# ----------------------------------------------------------------------
# 命令白名单（argv 不含解释器；{date}/{start}/{end}/{factor} 为占位符）
# ----------------------------------------------------------------------

REGIME_SNIPPET = ("from src.data.storage import Storage; "
                  "from src.pipeline.runner import run_regime_pipeline; "
                  "db=Storage(); db.init_db(); run_regime_pipeline(db)")

COMMANDS: list[dict] = [
    # ---- 每日流程 ----
    {"id": "daily", "group": "每日流程", "label": "一键每日全流程",
     "desc": "采集→因子→Regime→漂移→进化→日报→剧本→复盘（15:40后跑）",
     "argv": ["-m", "cli", "daily", "--date", "{date}"], "params": ["date"]},
    {"id": "brief_gen", "group": "每日流程", "label": "生成审视简报",
     "desc": "只读扫描 DB/日志 → reports/brief/latest.html",
     "argv": ["scripts/generate_brief.py"], "params": []},

    # ---- 数据 ----
    {"id": "collect_today", "group": "数据", "label": "采集今日数据",
     "desc": "涨停池/龙虎榜/资金流/新闻等全部数据源",
     "argv": ["-m", "cli.collect", "--today"], "params": []},
    {"id": "compute_today", "group": "数据", "label": "计算今日因子",
     "desc": "全部注册因子算值入库 factor_values",
     "argv": ["-m", "cli.backtest", "--compute-today"], "params": []},
    {"id": "regime", "group": "数据", "label": "Regime 识别",
     "desc": "市场状态识别并落库 regime_state",
     "argv": ["-c", REGIME_SNIPPET], "params": []},

    # ---- 因子研究 ----
    {"id": "checkup", "group": "因子研究", "label": "九因子体检",
     "desc": "事件研究批量体检：T+1/3/5 真实胜率/盈亏比/分段稳定性",
     "argv": ["-m", "cli.strategy", "event", "--checkup",
              "--start", "{start}", "--end", "{end}"], "params": ["start", "end"]},
    {"id": "event", "group": "因子研究", "label": "单因子事件研究",
     "desc": "指定因子触发样本的 T+N 收益分布（含 regime 分层）",
     "argv": ["-m", "cli.strategy", "event", "--factor", "{factor}",
              "--start", "{start}", "--end", "{end}"],
     "params": ["factor", "start", "end"]},
    {"id": "gate", "group": "因子研究", "label": "两段胜率验收门",
     "desc": "决策A：近60+近30两段胜率门，替代旧 IC 门",
     "argv": ["-m", "cli.strategy", "gate", "--factor", "{factor}",
              "--end", "{date}"], "params": ["factor", "date"]},
    {"id": "drift", "group": "因子研究", "label": "漂移检测",
     "desc": "全因子 IC 漂移报告",
     "argv": ["-m", "cli.drift", "--date", "{date}"], "params": ["date"]},
    {"id": "surgery", "group": "因子研究", "label": "因子解剖",
     "desc": "解剖单因子 IC 序列，诊断有效性来源",
     "argv": ["-m", "cli.mine", "surgery", "--factor", "{factor}"],
     "params": ["factor"]},
    {"id": "evolve", "group": "因子研究", "label": "因子进化 (3代×5)",
     "desc": "LLM 进化引擎挖掘新因子（需 API，可能较久）",
     "argv": ["-m", "cli.mine", "evolve", "--generations", "3", "--population", "5"],
     "params": []},

    # ---- 报告/信号 ----
    {"id": "report", "group": "报告与信号", "label": "生成日报",
     "argv": ["-m", "cli.report", "--date", "{date}"], "params": ["date"],
     "desc": "当日完整日报（文本）"},
    {"id": "signal", "group": "报告与信号", "label": "次日选股信号",
     "argv": ["-m", "cli.signal"], "params": [], "desc": "TOP10 候选（默认今天）"},
    {"id": "recommend", "group": "报告与信号", "label": "个股推荐",
     "argv": ["-m", "cli.recommend"], "params": [], "desc": "含买入点位（默认今天）"},
    {"id": "strategy_list", "group": "报告与信号", "label": "预置策略列表",
     "argv": ["-m", "cli.strategy", "list"], "params": [], "desc": "查看策略库"},
]

CMD_BY_ID = {c["id"]: c for c in COMMANDS}


# ----------------------------------------------------------------------
# 任务执行器（串行，单任务）
# ----------------------------------------------------------------------

class JobRunner:
    """同一时刻只允许一个子进程任务；输出按行缓存供前端轮询。"""

    def __init__(self, root: Path):
        self.root = root
        self.lock = threading.Lock()
        self.job: dict | None = None
        self.proc: subprocess.Popen | None = None

    def start(self, label: str, argv: list[str]) -> tuple[bool, str]:
        with self.lock:
            if self.job and self.job["status"] == "running":
                return False, f"忙：『{self.job['label']}』仍在运行，请等它结束或先停止"
            self.job = {"label": label, "status": "running", "rc": None,
                        "started": datetime.now().strftime("%H:%M:%S"),
                        "ended": None, "lines": []}
        threading.Thread(target=self._work, args=(argv,), daemon=True).start()
        return True, "已启动"

    def _work(self, argv: list[str]) -> None:
        env = {**os.environ, "PYTHONUTF8": "1", "PYTHONIOENCODING": "utf-8"}
        cmd = [sys.executable] + argv
        try:
            proc = subprocess.Popen(
                cmd, cwd=str(self.root), env=env,
                stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, encoding="utf-8", errors="replace", bufsize=1)
        except OSError as e:
            self._finish(-1, [f"[dashboard] 启动失败: {e}"])
            return
        with self.lock:
            self.proc = proc
        assert proc.stdout is not None
        for line in proc.stdout:
            with self.lock:
                lines = self.job["lines"]
                if len(lines) < MAX_LINES:
                    lines.append(line.rstrip("\n"))
                elif len(lines) == MAX_LINES:
                    lines.append(f"[dashboard] 输出超过 {MAX_LINES} 行，已截断")
        rc = proc.wait()
        self._finish(rc, [])

    def _finish(self, rc: int, extra: list[str]) -> None:
        with self.lock:
            self.job["lines"].extend(extra)
            self.job["rc"] = rc
            self.job["status"] = "done" if rc == 0 else "failed"
            self.job["ended"] = datetime.now().strftime("%H:%M:%S")
            self.proc = None

    def stop(self) -> str:
        with self.lock:
            proc = self.proc
        if proc is None:
            return "当前无运行中任务"
        if os.name == "nt":   # 终止整棵进程树（cli daily 会再开子进程）
            subprocess.run(["taskkill", "/PID", str(proc.pid), "/T", "/F"],
                           capture_output=True)
        else:
            proc.terminate()
        return "已发送停止指令"

    def status(self, since: int) -> dict:
        with self.lock:
            if self.job is None:
                return {"running": False, "label": None, "lines": [], "next": 0}
            lines = self.job["lines"]
            since = max(0, min(since, len(lines)))
            return {"running": self.job["status"] == "running",
                    "label": self.job["label"], "status": self.job["status"],
                    "rc": self.job["rc"], "started": self.job["started"],
                    "ended": self.job["ended"],
                    "lines": lines[since:], "next": len(lines)}


RUNNER = JobRunner(ROOT)


def build_argv(cmd: dict, params: dict[str, str]) -> tuple[list[str] | None, str]:
    """校验参数并代入占位符。返回 (argv, 错误信息)。"""
    values: dict[str, str] = {}
    naming = get_naming()
    for p in cmd.get("params", []):
        v = (params.get(p) or "").strip()
        if not v:
            return None, f"缺少参数: {p}"
        if p in ("date", "start", "end"):
            if not DATE_RE.match(v):
                return None, f"参数 {p} 须为 YYYY-MM-DD: {v}"
        elif p == "factor":
            if v not in naming.names():
                return None, f"未知因子: {v}"
        values[p] = v
    argv = [a.format(**values) if "{" in a else a for a in cmd["argv"]]
    return argv, ""


# ----------------------------------------------------------------------
# 只读概览
# ----------------------------------------------------------------------

def overview() -> list[dict]:
    """快速只读概览：关键表最新日期 / regime / DB 体积。失败如实显示。"""
    items: list[dict] = []
    if not DB_PATH.exists():
        return [{"k": "数据库", "v": "不存在"}]
    items.append({"k": "DB 体积", "v": f"{DB_PATH.stat().st_size / 1e6:.0f} MB"})
    try:
        conn = sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True)
        for label, table in [("行情最新", "daily_price"), ("涨停池最新", "zt_pool"),
                             ("因子值最新", "factor_values")]:
            try:
                v = conn.execute(
                    f"SELECT MAX(substr(trade_date,1,10)) FROM {table}").fetchone()[0]
                items.append({"k": label, "v": v or "空表"})
            except sqlite3.Error:
                items.append({"k": label, "v": "查询失败"})
        try:
            row = conn.execute("SELECT trade_date, regime_type FROM regime_state "
                               "ORDER BY trade_date DESC LIMIT 1").fetchone()
            items.append({"k": "Regime", "v": f"{row[1]} ({row[0]})" if row else "无"})
        except sqlite3.Error:
            items.append({"k": "Regime", "v": "查询失败"})
        conn.close()
    except sqlite3.Error as e:
        items.append({"k": "数据库", "v": f"打开失败: {e}"})
    if BRIEF_LATEST.exists():
        ts = datetime.fromtimestamp(BRIEF_LATEST.stat().st_mtime)
        items.append({"k": "最新简报", "v": ts.strftime("%m-%d %H:%M")})
    else:
        items.append({"k": "最新简报", "v": "尚未生成"})
    return items


# ----------------------------------------------------------------------
# 页面渲染（占位符替换，避免 format 与 CSS/JS 花括号冲突）
# ----------------------------------------------------------------------

PAGE = """<!DOCTYPE html>
<html lang="zh">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Alpha Miner 控制台</title>
<style>
  :root { color-scheme: light; }
  * { box-sizing: border-box; }
  body { font-family: "Microsoft YaHei", "PingFang SC", system-ui, sans-serif;
         max-width: 1080px; margin: 0 auto; padding: 20px 16px 60px;
         color: #1a1a2e; background: #fafafa; line-height: 1.55; }
  h1 { font-size: 21px; margin: 0 0 2px; }
  h2 { font-size: 15px; margin: 22px 0 8px; padding-bottom: 5px;
       border-bottom: 2px solid #e0e0e0; }
  .meta { color: #777; font-size: 12.5px; }
  .snap { display: flex; flex-wrap: wrap; gap: 8px; margin: 12px 0; }
  .snap .item { background: #fff; border: 1px solid #e3e3e3; border-radius: 8px;
                padding: 6px 12px; font-size: 13px; }
  .snap .item .k { color: #888; font-size: 11.5px; display: block; }
  .toolbar { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end;
             background: #fff; border: 1px solid #e3e3e3; border-radius: 8px;
             padding: 10px 14px; margin: 10px 0; }
  .toolbar label { font-size: 12px; color: #666; display: block; }
  .toolbar input, .toolbar select { font-size: 13px; padding: 4px 6px;
             border: 1px solid #ccc; border-radius: 5px; }
  .grid { display: flex; flex-wrap: wrap; gap: 8px; margin: 8px 0; }
  button.cmd { background: #fff; border: 1px solid #c9d4e0; border-radius: 8px;
               padding: 8px 12px; font-size: 13.5px; cursor: pointer;
               text-align: left; min-width: 180px; }
  button.cmd:hover { background: #eef4fb; border-color: #3a6ea5; }
  button.cmd:disabled { opacity: .45; cursor: not-allowed; }
  button.cmd .d { display: block; color: #888; font-size: 11.5px; font-weight: 400; }
  button.cmd .p { color: #a07b2a; font-size: 11px; }
  #status { font-size: 13.5px; margin: 8px 0; }
  #status .run { color: #a06000; } #status .ok { color: #2e7d46; }
  #status .fail { color: #c0392b; font-weight: 600; }
  #stopbtn { background: #fdecea; border: 1px solid #c0392b; color: #c0392b;
             border-radius: 6px; padding: 3px 12px; cursor: pointer; font-size: 12.5px;
             display: none; margin-left: 10px; }
  pre#out { background: #1e1e2e; color: #d8e2d8; padding: 12px 14px; min-height: 90px;
            max-height: 460px; overflow: auto; border-radius: 8px; font-size: 12.5px;
            white-space: pre-wrap; word-break: break-all; }
  table { border-collapse: collapse; width: 100%; font-size: 13px; background: #fff; }
  th, td { text-align: left; padding: 6px 10px; border-bottom: 1px solid #eee;
           vertical-align: top; }
  th { color: #666; font-weight: 600; background: #f4f4f4; }
  td.cn { font-weight: 600; white-space: nowrap; }
  td.en { font-family: Consolas, monospace; font-size: 12px; color: #555;
          white-space: nowrap; }
  .tag { display: inline-block; border-radius: 4px; padding: 0 7px; font-size: 11.5px; }
  .tag.alpha { background: #eef6ee; color: #2e7d46; border: 1px solid #cde5cd; }
  .tag.filter { background: #fdf3e7; color: #a06000; border: 1px solid #f0d5b0; }
  details summary { cursor: pointer; color: #3a6ea5; font-size: 12.5px; }
  details .detail { margin: 6px 0 2px; color: #333; font-size: 12.8px; }
  details .note { color: #8a6d1a; font-size: 12px; background: #fdf3d7;
                  border-radius: 4px; padding: 3px 8px; margin-top: 4px; }
  a { color: #3a6ea5; }
  .links { font-size: 13.5px; margin: 8px 0; }
</style>
</head>
<body>
<h1>Alpha Miner 控制台</h1>
<div class="meta">A股短线因子挖掘系统 · 本地控制台（127.0.0.1，仅本机可访问）· 页面生成于 __NOW__</div>

<h2>系统概览</h2>
<div class="snap">__OVERVIEW__</div>
<div class="links">
  📄 <a href="/brief" target="_blank">打开最新审视简报</a>
  &nbsp;·&nbsp; <a href="javascript:location.reload()">刷新概览</a>
</div>

<h2>参数（命令按钮标注了各自用到的参数）</h2>
<div class="toolbar">
  <div><label>日期 date</label><input type="date" id="p_date"></div>
  <div><label>开始 start</label><input type="date" id="p_start"></div>
  <div><label>结束 end</label><input type="date" id="p_end"></div>
  <div><label>因子 factor</label><select id="p_factor">__FACTOR_OPTIONS__</select></div>
</div>

__COMMAND_GROUPS__

<h2>运行输出 <button id="stopbtn" onclick="stopJob()">停止任务</button></h2>
<div id="status">空闲，点上方按钮执行命令。</div>
<pre id="out"></pre>

<h2>因子映射表（中文 ↔ 英文）</h2>
<table>
<thead><tr><th>中文名</th><th>英文名</th><th>类别</th><th>角色</th><th>说明</th></tr></thead>
<tbody>__FACTOR_ROWS__</tbody>
</table>

<script>
const today = new Date(), fmt = d => d.toISOString().slice(0,10);
document.getElementById('p_date').value = fmt(today);
document.getElementById('p_end').value = fmt(today);
document.getElementById('p_start').value = fmt(new Date(today - 90*86400e3));

let nextIdx = 0, polling = false;

function setRunning(on) {
  document.querySelectorAll('button.cmd').forEach(b => b.disabled = on);
  document.getElementById('stopbtn').style.display = on ? 'inline-block' : 'none';
}

async function runCmd(id) {
  const body = new URLSearchParams({cmd: id,
    date: document.getElementById('p_date').value,
    start: document.getElementById('p_start').value,
    end: document.getElementById('p_end').value,
    factor: document.getElementById('p_factor').value});
  const r = await fetch('/api/run', {method: 'POST', body});
  const d = await r.json();
  if (!d.ok) { document.getElementById('status').innerHTML =
      '<span class="fail">' + d.msg + '</span>'; return; }
  document.getElementById('out').textContent = '';
  nextIdx = 0; setRunning(true);
  if (!polling) poll();
}

async function poll() {
  polling = true;
  try {
    const r = await fetch('/api/status?since=' + nextIdx);
    const d = await r.json();
    if (d.lines && d.lines.length) {
      const out = document.getElementById('out');
      out.textContent += d.lines.join('\\n') + '\\n';
      out.scrollTop = out.scrollHeight;
    }
    nextIdx = d.next || 0;
    const st = document.getElementById('status');
    if (d.running) {
      st.innerHTML = '<span class="run">⏳ 运行中: ' + d.label +
                     '（' + d.started + ' 开始）</span>';
      setTimeout(poll, 1000); return;
    }
    if (d.label) {
      st.innerHTML = d.status === 'done'
        ? '<span class="ok">✓ 完成: ' + d.label + '（' + d.ended + '）</span>'
        : '<span class="fail">✗ 失败: ' + d.label + ' (exit ' + d.rc + ')</span>';
    }
  } catch (e) { /* 服务停了就静默 */ }
  polling = false; setRunning(false);
}

async function stopJob() {
  await fetch('/api/stop', {method: 'POST'});
}
poll();  // 页面打开时接上正在跑的任务
</script>
</body>
</html>
"""


def render_page() -> str:
    naming = get_naming()
    esc = html.escape

    ov = "".join(f'<div class="item"><span class="k">{esc(i["k"])}</span>'
                 f'{esc(str(i["v"]))}</div>' for i in overview())

    opts = "".join(f'<option value="{esc(r["en"])}">{esc(r["cn"])} ({esc(r["en"])})'
                   f'</option>' for r in naming.table())

    groups: dict[str, list[str]] = {}
    for c in COMMANDS:
        ps = c.get("params", [])
        hint = f'<span class="p">参数: {", ".join(ps)}</span>' if ps else ""
        groups.setdefault(c["group"], []).append(
            f'<button class="cmd" onclick="runCmd(\'{c["id"]}\')">'
            f'{esc(c["label"])} {hint}<span class="d">{esc(c.get("desc", ""))}'
            f'</span></button>')
    cmd_html = "".join(
        f"<h2>{esc(g)}</h2><div class=\"grid\">{''.join(btns)}</div>"
        for g, btns in groups.items())

    rows = []
    for r in naming.table():
        note = f'<div class="note">📌 {esc(r["note"])}</div>' if r["note"] else ""
        detail = (f'<details><summary>详细说明</summary>'
                  f'<div class="detail">{esc(r["detail"])}</div>{note}</details>'
                  if r["detail"] else "")
        role_tag = f'<span class="tag {esc(r["role"])}">{esc(r["role_cn"])}</span>'
        rows.append(f'<tr><td class="cn">{esc(r["cn"])}</td>'
                    f'<td class="en">{esc(r["en"])}</td>'
                    f'<td>{esc(r["category_cn"])}</td><td>{role_tag}</td>'
                    f'<td>{esc(r["desc"])}{detail}</td></tr>')

    return (PAGE
            .replace("__NOW__", datetime.now().strftime("%Y-%m-%d %H:%M"))
            .replace("__OVERVIEW__", ov)
            .replace("__FACTOR_OPTIONS__", opts)
            .replace("__COMMAND_GROUPS__", cmd_html)
            .replace("__FACTOR_ROWS__", "".join(rows)))


# ----------------------------------------------------------------------
# HTTP 服务
# ----------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, obj: dict, code: int = 200) -> None:
        self._send(code, json.dumps(obj, ensure_ascii=False).encode("utf-8"),
                   "application/json; charset=utf-8")

    def _html(self, text: str, code: int = 200) -> None:
        self._send(code, text.encode("utf-8"), "text/html; charset=utf-8")

    def do_GET(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        if url.path == "/":
            self._html(render_page())
        elif url.path == "/api/status":
            since = int((parse_qs(url.query).get("since") or ["0"])[0])
            self._json(RUNNER.status(since))
        elif url.path == "/brief":
            if BRIEF_LATEST.exists():
                self._send(200, BRIEF_LATEST.read_bytes(),
                           "text/html; charset=utf-8")
            else:
                self._html("<p>简报尚未生成。回控制台点『生成审视简报』。</p>", 404)
        elif url.path == "/api/factors":
            self._json({"factors": get_naming().table()})
        elif url.path == "/favicon.ico":
            self._send(204, b"", "image/x-icon")
        else:
            self._html("<p>404</p>", 404)

    def do_POST(self) -> None:  # noqa: N802
        url = urlparse(self.path)
        n = int(self.headers.get("Content-Length") or 0)
        form = {k: v[0] for k, v in
                parse_qs(self.rfile.read(n).decode("utf-8")).items()}
        if url.path == "/api/run":
            cmd = CMD_BY_ID.get(form.get("cmd", ""))
            if cmd is None:
                self._json({"ok": False, "msg": "未知命令"}, 400)
                return
            argv, err = build_argv(cmd, form)
            if argv is None:
                self._json({"ok": False, "msg": err}, 400)
                return
            ok, msg = RUNNER.start(cmd["label"], argv)
            self._json({"ok": ok, "msg": msg}, 200 if ok else 409)
        elif url.path == "/api/stop":
            self._json({"ok": True, "msg": RUNNER.stop()})
        else:
            self._json({"ok": False, "msg": "404"}, 404)

    def log_message(self, fmt: str, *args) -> None:  # 安静模式，只记错误
        if args and str(args[1] if len(args) > 1 else "").startswith("5"):
            super().log_message(fmt, *args)


def main() -> None:
    parser = argparse.ArgumentParser(description="Alpha Miner local dashboard")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--open", action="store_true", help="launch browser")
    args = parser.parse_args()

    server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    url = f"http://127.0.0.1:{args.port}/"
    print(f"[dashboard] serving at {url}  (Ctrl+C to quit)")  # ASCII: GBK console
    if args.open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("[dashboard] stopped")


if __name__ == "__main__":
    main()
