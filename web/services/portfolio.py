"""持仓管理: 本地JSON存储"""
import json
from pathlib import Path
from datetime import datetime

PORT_FILE = Path(__file__).parent.parent.parent / "data" / "portfolio.json"


def _load():
    if PORT_FILE.exists():
        return json.loads(PORT_FILE.read_text())
    return {"positions": [], "cash": 100000, "history": []}


def _save(data):
    PORT_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))


def get_portfolio() -> dict:
    return _load()


def add_position(code: str, name: str, price: float, shares: int,
                 stop: float, target: float, reason: str = ""):
    d = _load()
    # 检查重复
    for p in d["positions"]:
        if p["code"] == code:
            return False, "已持有该股票"
    cost = price * shares
    d["positions"].append({
        "code": code, "name": name,
        "buy_price": price, "shares": shares,
        "stop": stop, "target": target,
        "reason": reason,
        "buy_date": datetime.now().strftime("%Y-%m-%d"),
        "cost": cost,
    })
    d["cash"] -= cost
    d["history"].append({
        "action": "buy", "code": code, "name": name,
        "price": price, "shares": shares, "cost": cost,
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    _save(d)
    return True, "添加成功"


def remove_position(code: str, sell_price: float = 0):
    d = _load()
    pos = [p for p in d["positions"] if p["code"] == code]
    if not pos:
        return False, "未找到持仓"
    pos = pos[0]
    sell_price = sell_price or pos["buy_price"]
    revenue = sell_price * pos["shares"]
    pnl = (sell_price - pos["buy_price"]) * pos["shares"]
    d["positions"] = [p for p in d["positions"] if p["code"] != code]
    d["cash"] += revenue
    d["history"].append({
        "action": "sell", "code": code, "name": pos["name"],
        "price": sell_price, "shares": pos["shares"],
        "revenue": revenue, "pnl": round(pnl, 2),
        "time": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    _save(d)
    return True, f"卖出 {pos['name']} 盈亏 {pnl:+.0f}"


def update_stops(code: str, stop: float | None = None, target: float | None = None):
    d = _load()
    for p in d["positions"]:
        if p["code"] == code:
            if stop:
                p["stop"] = stop
            if target:
                p["target"] = target
            break
    _save(d)


# ---------- 自选股 ----------
WATCH_FILE = Path(__file__).parent.parent.parent / "data" / "watchlist.json"


def get_watchlist() -> list[str]:
    if WATCH_FILE.exists():
        return json.loads(WATCH_FILE.read_text())
    return []


def save_watchlist(codes: list[str]):
    WATCH_FILE.write_text(json.dumps(codes, ensure_ascii=False))


# ---------- 复盘日志 ----------
REVIEW_FILE = Path(__file__).parent.parent.parent / "data" / "reviews.json"


def get_reviews() -> list:
    if REVIEW_FILE.exists():
        return json.loads(REVIEW_FILE.read_text())
    return []


def add_review(date: str, content: str, tags: list[str] = None):
    reviews = get_reviews()
    reviews.append({
        "date": date,
        "content": content,
        "tags": tags or [],
        "created": datetime.now().strftime("%Y-%m-%d %H:%M"),
    })
    REVIEW_FILE.write_text(json.dumps(reviews, ensure_ascii=False, indent=2))


# ---------- 交易计划 ----------
def get_trade_plans() -> list:
    """从 portfolio.json 中读取交易计划列表"""
    d = _load()
    return d.get("trade_plans", [])


def get_latest_plan() -> dict | None:
    """获取最新的交易计划"""
    plans = get_trade_plans()
    if not plans:
        # 也尝试从 recommendations/ 目录读
        from pathlib import Path as P
        rec_dir = P(__file__).parent.parent.parent / "recommendations"
        plans_files = sorted(rec_dir.glob("*_tradeplan.json"), reverse=True)
        if plans_files:
            return json.loads(plans_files[0].read_text())
        return None
    # 返回 target_date 最新的
    plans.sort(key=lambda p: p.get("target_date", ""), reverse=True)
    return plans[0]
