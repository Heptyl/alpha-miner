"""A股实时行情采集器 — 通过腾讯API(Windows curl)获取

支持:
- 实时行情(最新价/盘口/换手率等)
- 批量获取(一次最多~50只)
- WSL2兼容(通过Windows curl绕行)
"""

import subprocess
import time
import json
import re
from typing import Optional
from pathlib import Path

CURL_PATH = "/mnt/c/Windows/System32/curl.exe"
CACHE_PATH = Path("output/trader/realtime_cache.json")


def _curl_tencent(codes: list[str], timeout: int = 10) -> dict:
    """通过Windows curl获取腾讯实时行情"""
    # 代码前缀映射: 6开头=sh, 0/3开头=sz, 8/9开头=bj, hk=hk, 5位数0开头=港股
    # 建立 原始code → prefixed 映射
    code_map = {}
    for c in codes:
        if c.startswith("hk") or c.startswith("r_hk") or c.startswith("H") or c.startswith("h"):
            clean = c.upper().lstrip("H")
            code_map[c] = f"hk{clean}"
        elif c.startswith("bj"):
            code_map[c] = c
        elif c.startswith(("6",)):
            code_map[c] = f"sh{c}"
        elif c.startswith(("8", "9")):
            code_map[c] = f"bj{c}"
        elif len(c) == 5 and c.startswith("0"):
            code_map[c] = f"hk{c}"
        else:
            code_map[c] = f"sz{c}"

    query = ",".join(code_map.values())
    url = f"http://qt.gtimg.cn/q={query}"
    
    try:
        r = subprocess.run(
            [CURL_PATH, "-s", "--max-time", str(timeout), url],
            capture_output=True, timeout=timeout + 5,
        )
        raw = r.stdout.decode("gbk", errors="replace")
    except Exception as e:
        return {"error": str(e)}
    
    result = {}
    for line in raw.strip().split(";"):
        line = line.strip()
        if not line or "=" not in line:
            continue
        key, _, val = line.partition("=")
        val = val.strip('"').strip()
        if not val:
            continue
        
        parts = val.split("~")
        if len(parts) < 50:
            continue
        
        code_raw = parts[2]  # 纯代码
        name = parts[1]
        
        try:
            quote = {
                "code": code_raw,
                "name": name,
                "price": _safe_float(parts[3]),
                "yesterday_close": _safe_float(parts[4]),
                "open": _safe_float(parts[5]),
                "volume": _safe_int(parts[6]),
                "high": _safe_float(parts[33]) if len(parts) > 33 else 0,
                "low": _safe_float(parts[34]) if len(parts) > 34 else 0,
                "amount_wan": _safe_float(parts[37]) if len(parts) > 37 else 0,  # 万元
                "timestamp": parts[30] if len(parts) > 30 else "",
                "turnover_rate": _safe_float(parts[38]) if len(parts) > 38 else 0,
                "pe": _safe_float(parts[39]) if len(parts) > 39 else 0,
                "change_pct": _safe_float(parts[32]) if len(parts) > 32 else 0,
                
                # 买卖五档 [价格, 量]
                "bid5": [_safe_float(parts[9]), _safe_int(parts[10])],
                "bid4": [_safe_float(parts[11]), _safe_int(parts[12])],
                "bid3": [_safe_float(parts[13]), _safe_int(parts[14])],
                "bid2": [_safe_float(parts[15]), _safe_int(parts[16])],
                "bid1": [_safe_float(parts[17]), _safe_int(parts[18])],  # 买一
                "ask1": [_safe_float(parts[19]), _safe_int(parts[20])],  # 卖一
                "ask2": [_safe_float(parts[21]), _safe_int(parts[22])],
                "ask3": [_safe_float(parts[23]), _safe_int(parts[24])],
                "ask4": [_safe_float(parts[25]), _safe_int(parts[26])],
                "ask5": [_safe_float(parts[27]), _safe_int(parts[28])],
            }
            quote["change_pct_calc"] = round(
                (quote["price"] / quote["yesterday_close"] - 1) * 100, 2
            ) if quote["yesterday_close"] > 0 else 0
            # 用code_raw反查原始输入code(同一个raw可能对应多个输入)
            original_code = code_raw
            for inp, pref in code_map.items():
                if pref.endswith(code_raw) or pref == f"hk{code_raw}":
                    original_code = inp
                    break
            result[original_code] = quote
        except (IndexError, ValueError):
            continue
    
    return result


def _safe_float(s: str) -> float:
    try:
        return float(s) if s else 0.0
    except (ValueError, TypeError):
        return 0.0


def _safe_int(s: str) -> int:
    try:
        return int(float(s)) if s else 0
    except (ValueError, TypeError):
        return 0


def get_realtime(codes: list[str]) -> dict:
    """获取实时行情，带缓存"""
    # Check cache (10秒内不重复请求)
    if CACHE_PATH.exists():
        try:
            cache = json.loads(CACHE_PATH.read_text())
            if time.time() - cache.get("ts", 0) < 10:
                cached = {k: v for k, v in cache.get("data", {}).items() if k in codes}
                if len(cached) == len(codes):
                    return cached
        except:
            pass
    
    # Fetch fresh
    data = _curl_tencent(codes)
    if "error" not in data:
        CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
        CACHE_PATH.write_text(json.dumps({"ts": time.time(), "data": data}, ensure_ascii=False))
    
    return data


def get_realtime_single(code: str) -> Optional[dict]:
    """获取单只股票实时行情"""
    data = get_realtime([code])
    return data.get(code)


# 用户持仓代码 — 统一从 portfolio.json 读取
from src.config.portfolio import get_portfolio_codes as _get_codes
USER_POSITIONS = _get_codes()


def get_user_positions_realtime() -> dict:
    """获取用户所有持仓的实时行情"""
    return get_realtime(USER_POSITIONS)


if __name__ == "__main__":
    # Test
    data = get_realtime(USER_POSITIONS)
    for code, q in data.items():
        if isinstance(q, dict) and "name" in q:
            print(f"{code} {q['name']}: {q['price']:.2f} ({q.get('change_pct_calc',0):+.2f}%) "
                  f"H:{q['high']:.2f} L:{q['low']:.2f} V:{q['volume']} "
                  f"换手:{q['turnover_rate']:.2f}% @ {q['timestamp']}")
