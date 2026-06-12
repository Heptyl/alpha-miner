"""统一持仓配置 — 全系统唯一的持仓数据源

所有模块（交易、信号、新闻、Web）都从这里读取持仓数据。
数据源: data/portfolio.json
兜底: 无JSON时返回空列表（不再硬编码默认值）

用法:
    from src.config.portfolio import get_portfolio, get_portfolio_map
    positions = get_portfolio()          # list[dict]
    code_map = get_portfolio_map()       # {code: info}
    aliases = get_portfolio_aliases()    # {code: [别名]}
    sectors = get_portfolio_sectors()    # {code: [板块关键词]}
"""

import json
from pathlib import Path
from functools import lru_cache

ROOT = Path(__file__).parent.parent.parent
PORTFOLIO_FILE = ROOT / "data" / "portfolio.json"


@lru_cache(maxsize=1)
def _load_raw() -> dict:
    """加载 portfolio.json 原始数据（带缓存）"""
    if PORTFOLIO_FILE.exists():
        return json.loads(PORTFOLIO_FILE.read_text(encoding="utf-8"))
    return {"positions": []}


def reload():
    """强制重新加载（持仓变更后调用）"""
    _load_raw.cache_clear()


def get_portfolio() -> list[dict]:
    """返回持仓列表，每项含: code, name, buy_price/cost, shares, stop_loss, target, reason, buy_date, sector(可选)
    
    兼容 portfolio.json 的两种格式:
      1. {"positions": [{code, name, buy_price, shares, stop, ...}, ...]}
      2. {"code": {name, cost, shares, ...}, ...}
    """
    raw = _load_raw()
    
    if "positions" in raw and isinstance(raw["positions"], list):
        result = []
        for pos in raw["positions"]:
            item = {
                "code": pos.get("code", ""),
                "name": pos.get("name", ""),
                "buy_price": pos.get("buy_price", pos.get("cost", 0)),
                "shares": pos.get("shares", 0),
                "stop_loss": pos.get("stop_loss", pos.get("stop", 0)),
                "target": pos.get("target", 0),
                "reason": pos.get("reason", ""),
                "buy_date": pos.get("buy_date", ""),
                "aliases": pos.get("aliases", []),
                "sectors": pos.get("sectors", []),
                "sector": pos.get("sector", ""),
            }
            # cost字段: portfolio.json里是总价 (e.g. 77305.80)
            cost_total = pos.get("cost", 0)
            if cost_total > 0 and item["shares"] > 0:
                # 判断是总价还是单价
                if cost_total / item["shares"] > 100:
                    item["cost_per_share"] = cost_total / item["shares"]
                else:
                    item["cost_per_share"] = cost_total
            else:
                item["cost_per_share"] = item["buy_price"]
            result.append(item)
        return result
    
    # 格式2: {code: info}
    result = []
    for code, info in raw.items():
        if isinstance(info, dict) and "name" in info:
            result.append({
                "code": code,
                "name": info.get("name", ""),
                "buy_price": info.get("buy_price", info.get("cost", 0)),
                "shares": info.get("shares", 0),
                "stop_loss": info.get("stop_loss", 0),
                "target": info.get("target", 0),
                "reason": info.get("reason", ""),
                "buy_date": info.get("buy_date", ""),
                "cost_per_share": info.get("buy_price", info.get("cost", 0)),
                "aliases": info.get("aliases", []),
                "sectors": info.get("sectors", []),
                "sector": info.get("sector", ""),
            })
    return result


def get_portfolio_map() -> dict[str, dict]:
    """返回 {code: {name, cost, shares, stop_loss, ...}} 字典
    
    兼容旧代码的 dict 格式 (key=code)。
    """
    return {p["code"]: p for p in get_portfolio()}


def get_portfolio_aliases() -> dict[str, list[str]]:
    """返回 {code: [别名列表]} — 用于新闻匹配"""
    result = {}
    for p in get_portfolio():
        code = p["code"]
        name = p["name"]
        aliases = list(p.get("aliases") or [])
        if name and name not in aliases:
            aliases.insert(0, name)
        result[code] = aliases
    return result


def get_portfolio_sectors() -> dict[str, list[str]]:
    """返回 {code: [板块关键词]} — 用于板块新闻关联。
    
    从 concept_mapping 动态读取行业分类，并合并本机持仓配置中的关键词。
    """
    import sqlite3
    from src.data.storage import Storage

    # 从DB读行业分类
    db_sectors = {}
    try:
        conn = sqlite3.connect("data/alpha_miner.db")
        codes = [p["code"] for p in get_portfolio()]
        for code in codes:
            rows = conn.execute(
                "SELECT concept_name FROM concept_mapping WHERE stock_code=?", (code,)
            ).fetchall()
            db_sectors[code] = [r[0] for r in rows]
        conn.close()
    except Exception:
        pass

    # 合并：本机配置 + DB行业 + 持仓别名
    result = {}
    aliases_map = get_portfolio_aliases()
    portfolio_map = get_portfolio_map()
    for code in get_portfolio_codes():
        keywords = []
        position = portfolio_map.get(code, {})
        keywords.extend(position.get("sectors") or [])
        if position.get("sector"):
            keywords.append(position["sector"])
        # 2) DB行业分类（去重）
        for s in db_sectors.get(code, []):
            # 行业名简化：取最后几个字
            short = s.replace("制造业", "").replace("服务业", "").replace("和", "")
            if short and short not in keywords:
                keywords.append(short)
            if s not in keywords:
                keywords.append(s)
        # 3) 持仓别名（股名本身也是关键词）
        keywords.extend(aliases_map.get(code, []))
        result[code] = keywords
    return result


def get_cash() -> float:
    """返回账户现金"""
    raw = _load_raw()
    return raw.get("cash", 0)


def get_portfolio_codes() -> list[str]:
    """获取持仓代码列表 — 供 realtime_quote 等模块使用"""
    return [p["code"] for p in get_portfolio()]


# ============================================================
# 兼容旧接口 — 逐步迁移后可删除
# ============================================================

def get_legacy_portfolio_dict() -> dict:
    """返回旧格式的 {code: {name, shares, cost, stop_loss}} 字典
    供 plan_generator / intraday_signal / signal_monitor 使用。
    """
    result = {}
    for p in get_portfolio():
        result[p["code"]] = {
            "name": p["name"],
            "shares": p["shares"],
            "cost": p["buy_price"],
            "stop_loss": p["stop_loss"],
        }
    return result


def get_legacy_portfolio_list() -> list[dict]:
    """返回旧格式的 [{name, code, cost, shares, sector, stop_loss}] 列表
    供 trader_brief / eastmoney_news 使用。
    """
    return [
        {
            "name": p["name"],
            "code": p["code"],
            "cost": p["buy_price"],
            "shares": p["shares"],
            "sector": p.get("sector", ""),
            "stop_loss": p["stop_loss"],
        }
        for p in get_portfolio()
    ]


def get_legacy_name_map() -> dict[str, str]:
    """返回 {name: code} 供新闻匹配用
    供 eastmoney_news.PORTFOLIO_MAP 使用。
    """
    return {p["name"]: p["code"] for p in get_portfolio()}
