"""market_perception.py — 市场感知模块

接口:
  perceive_market() -> dict     # 盘面感知(板块排名/资金流/风格)
  perceive_stock(code) -> dict  # 个股感知(板块/封板/涨因)
  detect_reversal() -> dict     # 反转信号检测

数据源: 东财push2 API, 降级链 requests → curl.exe → curl
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger("trading_daemon")

# ── 东财push2 API ──
_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Referer": "https://quote.eastmoney.com/center/gridlist.html",
}

# 涨跌家数(上证+深证+创业板+中小100)
_ULIST_URL = (
    "http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2"
    "&fields=f104,f105,f14&secids=1.000001,0.399001,0.399006,0.399005"
)
# 跌停股列表(按涨幅升序, 前500)
_DT_URL = (
    "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=500&np=1&fltt=2&invt=2"
    "&fid=f3&po=0&fs=m:0+t:6+f:!2,m:0+t:80+f:!2,m:1+t:2+f:!2,m:1+t:23+f:!2"
    "&fields=f2,f3,f12,f14"
)
# 权重股: 银行+券商
_WEIGHT_CODES = [
    "601398", "601288", "600036", "601166", "600016",
    "600030", "601211", "601688",
]
_WEIGHT_SECIDS = ",".join(f"1.{c}" if c.startswith("6") else f"0.{c}" for c in _WEIGHT_CODES)
_WEIGHT_URL = (
    f"http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2"
    f"&fields=f3,f12,f14&secids={_WEIGHT_SECIDS}"
)
# 行业板块行情(涨幅排名)
_SECTOR_URL = (
    "http://push2.eastmoney.com/api/qt/clist/get?pn=1&pz=100&np=1&fltt=2&invt=2"
    "&fid=f3&po=1&fs=m:90+t:2+f:!50&fields=f3,f14,f104,f105,f6"
)

# ── 日内状态 ──
_session_cache: dict = {}


def _reset_if_new_day():
    today = datetime.now().strftime("%Y-%m-%d")
    if _session_cache.get("_date") != today:
        _session_cache.clear()
        _session_cache.update({
            "_date": today,
            "min_ratio": 100.0,
            "max_dt_count": 0,
            "reversal_fired": False,
        })


def _fetch_json(url: str) -> Optional[dict]:
    """请求东财API: requests → curl.exe → curl"""
    import requests
    for attempt in range(2):
        try:
            r = requests.get(url, headers=_HEADERS, timeout=10)
            data = r.json()
            if data.get("data"):
                return data
        except Exception:
            time.sleep(0.3)

    curl = shutil.which("curl.exe") or shutil.which("curl")
    if not curl:
        return None
    for attempt in range(2):
        try:
            r = subprocess.run(
                [curl, "-s", "--max-time", "10",
                 "-H", "User-Agent: Mozilla/5.0 (Windows NT 10.0; Win64; x64)", url],
                capture_output=True, timeout=15,
            )
            if len(r.stdout) > 50:
                return json.loads(r.stdout)
        except Exception:
            time.sleep(0.3)
    return None


def _get_up_down_ratio() -> float:
    """当前涨跌比(%)"""
    data = _fetch_json(_ULIST_URL)
    if not data:
        return -1.0
    up = down = 0
    for item in data["data"].get("diff", []):
        up += int(item.get("f104", 0))
        down += int(item.get("f105", 0))
    if up + down == 0:
        return -1.0
    return up / (up + down) * 100


def _get_dt_count() -> int:
    """当前跌停数(API)"""
    data = _fetch_json(_DT_URL)
    if not data:
        return -1
    items = data["data"].get("diff", {})
    if isinstance(items, dict):
        items = list(items.values())
    count = 0
    for x in items:
        if not isinstance(x, dict):
            continue
        pct = x.get("f3", 0)
        if not isinstance(pct, (int, float)):
            continue
        if pct > 100:
            pct = pct / 100
        code = str(x.get("f12", ""))
        if code.startswith(("688", "300", "301")):
            if pct <= -19.0:
                count += 1
        else:
            if pct <= -9.0:
                count += 1
    return count


def _get_dt_count_db() -> int:
    """当前跌停数(DB fallback, 用daily_price最新交易日数据)"""
    try:
        import sqlite3
        db_path = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"
        if not db_path.exists():
            return _get_dt_count()
        conn = sqlite3.connect(str(db_path))
        try:
            latest = conn.execute(
                "SELECT MAX(trade_date) FROM daily_price"
            ).fetchone()[0]
            if not latest:
                return -1
            # 主板: 跌幅<=-9.5%视为跌停(含ST可能跌5%)
            # 创业板/科创板: 跌幅<=-19.5%
            rows = conn.execute("""
                SELECT stock_code,
                       (close - pre_close) / pre_close * 100 as chg
                FROM daily_price
                WHERE trade_date = ? AND pre_close > 0
            """, (latest,)).fetchall()
            conn.close()
            count = 0
            for code, chg in rows:
                if chg is None:
                    continue
                if code.startswith(("688", "300", "301")):
                    if chg <= -19.5:
                        count += 1
                else:
                    if chg <= -9.5:
                        count += 1
            return count
        except Exception:
            conn.close()
            return -1
    except Exception:
        return _get_dt_count()


def _get_weight_red_pct() -> tuple[float, str]:
    """权重股翻红比例 → (百分比, 详情str)"""
    data = _fetch_json(_WEIGHT_URL)
    if not data:
        return -1.0, ""
    items = data["data"].get("diff", [])
    if isinstance(items, dict):
        items = list(items.values())
    red = total = 0
    details = []
    for x in items:
        if not isinstance(x, dict):
            continue
        pct = x.get("f3", 0)
        if not isinstance(pct, (int, float)):
            continue
        name = x.get("f14", "")
        total += 1
        if pct > 0:
            red += 1
            details.append(f"{name}+{pct:.1f}%")
    if total == 0:
        return 0.0, ""
    return red / total * 100, "/".join(details[:4])


# ═══════════════════════════════════════════════════════════
# 公开接口
# ═══════════════════════════════════════════════════════════


def perceive_market(daemon_ratio: float = -1) -> dict:
    """盘面感知 — 板块排名/资金流向/市场风格

    Args:
        daemon_ratio: daemon情绪模块已算好的涨跌比(%), 优先使用。
                      传入>=0则直接用, 否则自行查询(东财API, WSL下可能失败)。

    Returns:
        {
            "top5_up_sectors": [...],    # 涨幅前5 [{name, pct, net_yi}]
            "top5_down_sectors": [...],  # 跌幅前5
            "top_inflow": [...],         # 净流入前3板块
            "style": str,               # 大盘强/小盘强/均衡
            "ratio_now": float,
            "weight_red_pct": float,
        }
    """
    if daemon_ratio >= 0:
        ratio_now = daemon_ratio
    else:
        ratio_now = _get_up_down_ratio()
    weight_pct, _ = _get_weight_red_pct()

    # 板块排名(东财行业板块)
    top5_up, top5_down, top_inflow = _sector_rankings()

    return {
        "top5_up_sectors": top5_up,
        "top5_down_sectors": top5_down,
        "top_inflow": top_inflow,
        "style": _detect_style(),
        "ratio_now": round(ratio_now, 1) if ratio_now >= 0 else -1,
        "weight_red_pct": round(weight_pct, 1) if weight_pct >= 0 else -1,
    }


def perceive_stock(code: str) -> dict:
    """个股感知 — 板块联动/封板力度/LLM涨因

    Args:
        code: 6位股票代码

    Returns:
        {
            "sector": str,             # 所属行业
            "sector_chg": float,       # 板块今日涨跌%
            "seal_strength": float,    # 封单/流通市值(%)
            "logic": str,              # LLM判断涨因
            "logic_confidence": float, # 0-1
        }
    """
    result = {
        "sector": "",
        "sector_chg": 0,
        "seal_strength": 0,
        "logic": "",
        "logic_confidence": 0,
    }

    # 1. 所属行业(东财stock API)
    sector = _get_stock_industry(code)
    result["sector"] = sector

    # 2. 行业板块今日涨跌
    if sector:
        result["sector_chg"] = _get_sector_chg(sector)

    # 3. 封板力度(涨停票查封单)
    result["seal_strength"] = _get_seal_strength(code)

    # 4. LLM涨因分析
    logic, conf = _llm_analyze_logic(code)
    result["logic"] = logic
    result["logic_confidence"] = conf

    return result


def detect_reversal(market_data: dict = None) -> dict:
    """反转信号检测

    满足2/3条件触发:
      1. 涨跌比从≤20%回升到25%+
      2. 跌停从≥30减少到≤20
      3. 权重股≥60%翻红

    Args:
        market_data: perceive_market()的返回值, 传入时复用其数据不再调API

    Returns:
        {"signal", "reason", "ratio_now", "ratio_min", "dt_count", "weight_red_pct"}
    """
    _reset_if_new_day()

    # 复用perceive_market()已获取的数据, 避免重复API调用
    if market_data:
        ratio_now = market_data.get("ratio_now", -1)
        weight_pct = market_data.get("weight_red_pct", -1)
    else:
        ratio_now = _get_up_down_ratio()
        weight_pct, _ = _get_weight_red_pct()

    dt_count = _get_dt_count_db()

    if ratio_now < 0:
        return {
            "signal": "无信号", "reason": "涨跌比数据不足",
            "ratio_now": ratio_now, "ratio_min": _session_cache.get("min_ratio", -1),
            "dt_count": dt_count, "weight_red_pct": weight_pct,
        }

    if ratio_now >= 0:
        _session_cache["min_ratio"] = min(_session_cache["min_ratio"], ratio_now)
    if dt_count >= 0:
        _session_cache["max_dt_count"] = max(_session_cache["max_dt_count"], dt_count)

    signals = 0
    reasons = []

    if _session_cache["min_ratio"] <= 20 and ratio_now >= 25:
        signals += 1
        reasons.append(f"涨跌比{ratio_now:.0f}%(最低{_session_cache['min_ratio']:.0f}%)")

    if dt_count >= 0 and _session_cache["max_dt_count"] >= 30 and dt_count <= 20:
        signals += 1
        reasons.append(f"跌停{dt_count}只(最高{_session_cache['max_dt_count']}只)")

    if weight_pct >= 60:
        signals += 1
        reasons.append(f"权重{weight_pct:.0f}%翻红")

    if signals >= 2 and not _session_cache["reversal_fired"]:
        _session_cache["reversal_fired"] = True
        signal = "冰点反转中"
        logger.info(f"[反转信号] {signal}: {'; '.join(reasons)}")
    elif _session_cache["min_ratio"] <= 25 or (dt_count >= 0 and dt_count >= 30):
        signal = "继续冰点"
    else:
        signal = "无信号"

    return {
        "signal": signal,
        "reason": "; ".join(reasons) if reasons else "未满足反转条件",
        "ratio_now": round(ratio_now, 1),
        "ratio_min": round(_session_cache["min_ratio"], 1),
        "dt_count": dt_count,
        "weight_red_pct": round(weight_pct, 1) if weight_pct >= 0 else -1,
    }


# ═══════════════════════════════════════════════════════════
# 内部函数
# ═══════════════════════════════════════════════════════════


def _sector_rankings() -> tuple[list, list, list]:
    """板块涨幅前5/跌幅前5/净流入前3

    优先用DB(stock_industry_mapping + daily_price)聚合,
    东财API作为盘中实时备选(WSL下API不稳定).
    """
    # 1) 先尝试DB聚合(稳定可靠)
    db_result = _sector_rankings_from_db()
    if db_result[0]:  # 有数据就直接用
        return db_result

    # 2) DB无数据(可能是盘前/数据未采集) → 尝试东财API
    return _sector_rankings_from_api()


def _sector_rankings_from_db() -> tuple[list, list, list]:
    """从stock_industry_mapping + daily_price聚合板块排名"""
    try:
        import sqlite3
        db_path = Path(__file__).resolve().parents[2] / "data" / "alpha_miner.db"
        if not db_path.exists():
            return [], [], []
        conn = sqlite3.connect(str(db_path))
        try:
            latest = conn.execute(
                "SELECT MAX(trade_date) FROM daily_price"
            ).fetchone()[0]
            if not latest:
                return [], [], []

            rows = conn.execute("""
                SELECT sim.industry_name,
                       COUNT(*) as cnt,
                       ROUND(AVG(CASE
                           WHEN dp.pre_close > 0
                                AND ABS((dp.close - dp.pre_close) / dp.pre_close * 100) < 25
                           THEN (dp.close - dp.pre_close) / dp.pre_close * 100
                       END), 2) as avg_chg,
                       ROUND(SUM(dp.amount) / 1e8, 2) as total_amount_yi
                FROM daily_price dp
                JOIN stock_industry_mapping sim ON dp.stock_code = sim.stock_code
                WHERE dp.trade_date = ?
                GROUP BY sim.industry_name
                HAVING cnt >= 5
                ORDER BY avg_chg DESC
            """, (latest,)).fetchall()
        finally:
            conn.close()

        if not rows:
            return [], [], []

        all_sectors = []
        for r in rows:
            name, cnt, avg_chg, amount_yi = r
            if avg_chg is None:
                continue
            all_sectors.append({
                "name": name, "pct": avg_chg,
                "net_yi": amount_yi or 0,
                "stock_count": cnt,
            })

        top5_up = sorted(all_sectors, key=lambda s: s["pct"], reverse=True)[:5]
        top5_down = sorted(all_sectors, key=lambda s: s["pct"])[:5]
        top_inflow = sorted(all_sectors, key=lambda s: s["net_yi"], reverse=True)[:3]

        return top5_up, top5_down, top_inflow
    except Exception as e:
        logger.debug(f"DB板块排名失败: {e}")
        return [], [], []


def _sector_rankings_from_api() -> tuple[list, list, list]:
    """东财API获取板块排名(备选)"""
    data = _fetch_json(_SECTOR_URL)
    if not data:
        return [], [], []

    items = data["data"].get("diff", {})
    if isinstance(items, dict):
        items = list(items.values())

    all_sectors = []
    for x in items:
        if not isinstance(x, dict):
            continue
        name = x.get("f14", "")
        pct = x.get("f3", 0)
        net = x.get("f6", 0)
        if not name or not isinstance(pct, (int, float)):
            continue
        net_yi = net / 1e8 if isinstance(net, (int, float)) else 0
        all_sectors.append({"name": name, "pct": round(pct, 2), "net_yi": round(net_yi, 2)})

    if not all_sectors:
        return [], [], []

    top5_up = sorted(all_sectors, key=lambda s: s["pct"], reverse=True)[:5]
    top5_down = sorted(all_sectors, key=lambda s: s["pct"])[:5]
    top_inflow = sorted(all_sectors, key=lambda s: s["net_yi"], reverse=True)[:3]

    return top5_up, top5_down, top_inflow


def _detect_style() -> str:
    """大盘vs小盘风格"""
    try:
        big_codes = {"601398", "601318", "600519"}
        small_codes = {"002415", "300750", "300059"}
        secids = "1.601398,1.601318,1.600519,0.002415,0.300750,0.300059"
        data = _fetch_json(
            f"http://push2.eastmoney.com/api/qt/ulist.np/get?fltt=2&invt=2"
            f"&fields=f3,f12,f14&secids={secids}"
        )
        if not data:
            return "均衡"
        items = data["data"].get("diff", [])
        if isinstance(items, dict):
            items = list(items.values())
        big_sum = small_sum = 0
        for x in items:
            if not isinstance(x, dict):
                continue
            pct = x.get("f3", 0)
            if not isinstance(pct, (int, float)):
                continue
            code = str(x.get("f12", ""))
            if code in big_codes:
                big_sum += pct
            elif code in small_codes:
                small_sum += pct
        if big_sum - small_sum > 1.5:
            return "大盘强"
        elif small_sum - big_sum > 1.5:
            return "小盘强"
        return "均衡"
    except Exception:
        return "均衡"


def _get_stock_industry(code: str) -> str:
    """用东财stock API获取个股所属行业"""
    try:
        secid = f"1.{code}" if code.startswith("6") else f"0.{code}"
        data = _fetch_json(
            f"http://push2.eastmoney.com/api/qt/stock/get?secid={secid}&fields=f127,f14"
        )
        if data and data.get("data"):
            return data["data"].get("f127", "") or ""
    except Exception:
        pass
    return ""


def _get_sector_chg(sector_name: str) -> float:
    """查询行业板块今日涨跌"""
    try:
        data = _fetch_json(_SECTOR_URL)
        if not data:
            return 0
        items = data["data"].get("diff", {})
        if isinstance(items, dict):
            items = list(items.values())
        for x in items:
            if isinstance(x, dict) and x.get("f14") == sector_name:
                pct = x.get("f3", 0)
                return round(pct, 2) if isinstance(pct, (int, float)) else 0
    except Exception:
        pass
    return 0


def _get_seal_strength(code: str) -> float:
    """封板力度: 封单金额/流通市值(%)。仅涨停票有数据"""
    try:
        secid = f"1.{code}" if code.startswith("6") else f"0.{code}"
        # 涨停详情: f66=封单金额, f20=流通市值, f3=涨幅
        data = _fetch_json(
            f"http://push2.eastmoney.com/api/qt/stock/get?secid={secid}"
            f"&fields=f3,f66,f20,f9"
        )
        if not data or not data.get("data"):
            return 0
        d = data["data"]
        pct = d.get("f3", 0)
        if not isinstance(pct, (int, float)) or pct < 9:
            return 0  # 非涨停票无封单
        seal = d.get("f66", 0)  # 封单金额(元)
        mv = d.get("f20", 0)    # 流通市值(元)
        if isinstance(seal, (int, float)) and isinstance(mv, (int, float)) and mv > 0:
            return round(seal / mv * 100, 2)
    except Exception:
        pass
    return 0


def _llm_analyze_logic(code: str) -> tuple[str, float]:
    """LLM分析候选股的上涨逻辑 → (logic_str, confidence)"""
    try:
        from src.trader.daemon_db import _get_conn
        conn = _get_conn()
        rows = conn.execute(
            "SELECT title FROM news WHERE stock_code=? "
            "ORDER BY publish_time DESC LIMIT 5",
            (code,),
        ).fetchall()
        conn.close()
        headlines = [r[0] for r in rows if r[0]]
        if not headlines:
            return "", 0

        from src.agent.llm_client import get_client
        client = get_client()
        prompt = (
            "分析以下股票新闻标题，判断上涨的主要逻辑。\n"
            "只回答JSON格式: {\"logic\": \"概念炒作|业绩驱动|政策利好|资金推动|其他\", \"confidence\": 0.8}\n"
            "confidence范围0-1，表示判断把握度。\n"
            "新闻:\n" + "\n".join(f"- {t}" for t in headlines[:5])
        )
        resp = client.chat(prompt, max_tokens=100)
        # 解析JSON
        import re
        m = re.search(r'\{[^}]+\}', resp)
        if m:
            parsed = json.loads(m.group())
            return parsed.get("logic", "")[:20], parsed.get("confidence", 0.5)
        return resp.strip()[:20], 0.3
    except Exception:
        return "", 0
