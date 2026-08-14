"""资金流向数据采集 — 同花顺全市场排名接口。

主源: 同花顺个股资金流排名 (data.10jqka.com.cn/funds/ggzjl/)
      4 路并发拉取全量（~5200只、约110页，通常数秒完成）
回退: stock_individual_fund_flow (东方财富逐只，仅WAF严重时降级)

同花顺接口稳定，不走东财WAF，返回：代码、名称、涨跌幅、资金流入/流出/净额、成交额。
无超大单/大单拆分，但有主力净额（净额即主力净流入）。
"""

import concurrent.futures
import logging
import re
import time
from functools import lru_cache

import pandas as pd
import py_mini_racer
import requests
from akshare.datasets import get_ths_js
from bs4 import BeautifulSoup

from src.data.storage import Storage

logger = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Referer": "http://data.10jqka.com.cn/funds/ggzjl/",
    "Accept": "text/html, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
}

_MAX_WORKERS = 4  # 实测可稳定并发；继续增大容易触发同花顺限流


@lru_cache(maxsize=1)
def _get_ths_js_content() -> str:
    """通过 AkShare 自己的数据定位器读取 ths.js，兼容 Windows/Linux/uv。"""
    with open(get_ths_js("ths.js"), encoding="utf-8") as f:
        return f.read()


def _get_ths_v_code() -> str:
    """获取同花顺 hexin-v 验证码。"""
    js_code = py_mini_racer.MiniRacer()
    js_code.eval(_get_ths_js_content())
    return js_code.call("v")


def _parse_amount(text: str) -> float:
    """解析同花顺金额字符串为元，与 daily_price.amount 保持同一单位。"""
    text = text.strip()
    if not text or text == "0.00" or text == "-":
        return 0.0
    m = re.match(r"([+-]?\d+\.?\d*)\s*(亿|万)?", text)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2)
    if unit == "亿":
        return val * 100_000_000
    if unit == "万":
        return val * 10_000
    return val


def _fetch_ths_page(page: int, trade_date: str, retries: int = 3) -> list[dict]:
    """拉取并解析一页；每次尝试生成新 hexin-v，避免令牌失效。"""
    url = (
        "http://data.10jqka.com.cn/funds/ggzjl/"
        f"field/zdf/order/desc/page/{page}/ajax/1/free/1/"
    )
    for attempt in range(retries):
        try:
            headers = {**_HEADERS, "hexin-v": _get_ths_v_code()}
            response = requests.get(url, headers=headers, timeout=(5, 15))
            response.raise_for_status()
            table = BeautifulSoup(response.text, features="lxml").find("table")
            if table is None:
                raise ValueError("响应中没有 table（可能触发反爬）")
            rows = _parse_table(table, trade_date)
            if not rows:
                raise ValueError("响应 table 为空")
            return rows
        except Exception as exc:
            if attempt == retries - 1:
                logger.warning("fund_flow ths 第%d页失败: %s", page, exc)
                return []
            time.sleep(0.25 * (2 ** attempt))
    return []


def _fetch_ths_rank(
    trade_date: str,
    retries: int = 3,
    max_workers: int = _MAX_WORKERS,
) -> pd.DataFrame:
    """从同花顺拉全市场个股资金流排名。

    返回 DataFrame，列: stock_code, trade_date, stock_name, pct_change,
    inflow, outflow, net_amount, amount, main_net
    金额单位：元。
    """
    try:
        first_rows = _fetch_ths_page(1, trade_date, retries=retries)
    except Exception as e:
        logger.warning("fund_flow ths 第1页请求失败: %s", e)
        return pd.DataFrame()

    if not first_rows:
        return pd.DataFrame()
    all_rows = list(first_rows)

    # 先请求第1页获取总页数
    url_tpl = (
        "http://data.10jqka.com.cn/funds/ggzjl/"
        "field/zdf/order/desc/page/{page}/ajax/1/free/1/"
    )

    # 单独请求首页响应以读取页数；失败时用市场规模估计值。
    try:
        headers = {**_HEADERS, "hexin-v": _get_ths_v_code()}
        response = requests.get(url_tpl.format(page=1), headers=headers, timeout=(5, 15))
        response.raise_for_status()
        soup = BeautifulSoup(response.text, features="lxml")
        page_info = soup.find("span", attrs={"class": "page_info"})
        total_pages = int(page_info.text.split("/")[1]) if page_info else 110
    except Exception:
        total_pages = 110

    logger.info("fund_flow ths: 共 %d 页", total_pages)
    max_pages = min(total_pages, 120)  # 全市场 ~5200 只（~110 页）

    page_rows: dict[int, list[dict]] = {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_pages = {
            executor.submit(_fetch_ths_page, page, trade_date, retries): page
            for page in range(2, max_pages + 1)
        }
        for future in concurrent.futures.as_completed(future_pages):
            page = future_pages[future]
            try:
                rows = future.result()
            except Exception as exc:
                logger.warning("fund_flow ths 第%d页任务失败: %s", page, exc)
                rows = []
            if rows:
                page_rows[page] = rows

    for page in sorted(page_rows):
        all_rows.extend(page_rows[page])

    completed_pages = 1 + len(page_rows)
    completeness = completed_pages / max_pages
    if completeness < 0.8:
        logger.error(
            "fund_flow ths 完整度过低: %d/%d页 (%.1f%%)",
            completed_pages, max_pages, completeness * 100,
        )
        return pd.DataFrame()
    if completeness < 1:
        logger.warning(
            "fund_flow ths 部分页失败: %d/%d页 (%.1f%%)",
            completed_pages, max_pages, completeness * 100,
        )

    if not all_rows:
        return pd.DataFrame()

    logger.info("fund_flow ths 完成: 共 %d 只", len(all_rows))
    return pd.DataFrame(all_rows)


def _parse_table(table, trade_date: str) -> list[dict]:
    """解析同花顺 HTML 表格为一行 dict 列表。"""
    rows = []
    tbody = table.find("tbody")
    if not tbody:
        return rows

    for tr in tbody.find_all("tr"):
        tds = [td.get_text(strip=True) for td in tr.find_all("td")]
        # 期望: 序号, 股票代码, 股票简称, 最新价, 涨跌幅, 换手率, 流入, 流出, 净额, 成交额
        if len(tds) < 10:
            continue
        try:
            rows.append({
                "stock_code": tds[1],
                "trade_date": trade_date,
                "stock_name": tds[2],
                "pct_change": float(tds[4].replace("%", "")),
                "turnover_rate": float(tds[5].replace("%", "")),
                "inflow": _parse_amount(tds[6]),
                "outflow": _parse_amount(tds[7]),
                "net_amount": _parse_amount(tds[8]),
                "amount": _parse_amount(tds[9]),
                "main_net": _parse_amount(tds[8]),  # 同花顺净额即主力净流入
            })
        except (ValueError, IndexError):
            continue

    return rows


def fetch(trade_date: str, retries: int = 3, db: Storage | None = None) -> pd.DataFrame:
    """拉取资金流向。

    主源: 同花顺全市场排名（4 路分页并发）
    """
    # 主源：同花顺
    result = _fetch_ths_rank(trade_date, retries=retries)
    if not result.empty:
        return result

    # 回退：东财逐只（可能被 WAF）
    logger.warning("fund_flow: 同花顺失败，回退东财逐只")
    return _fetch_em_fallback(trade_date, db=db)


def _fetch_em_fallback(trade_date: str, db: Storage | None = None) -> pd.DataFrame:
    """回退：东财逐只资金流（只拉涨停+龙虎榜，限流更严）。"""
    import akshare as ak

    codes = _get_priority_codes(trade_date, db=db)
    if not codes:
        return pd.DataFrame()

    all_rows = []
    consecutive_fail = 0

    for code in codes:
        if consecutive_fail >= 15:
            logger.warning("fund_flow 东财连续失败15只，终止")
            break
        time.sleep(2)  # 更保守的限流
        try:
            prefix = "sh" if code.startswith("6") else "sz"
            df = ak.stock_individual_fund_flow(stock=code, market=prefix)
            if df is not None and not df.empty:
                latest = df.iloc[-1]
                row = {
                    "stock_code": code,
                    "trade_date": trade_date,
                    "stock_name": "",
                    "pct_change": 0.0,
                    "turnover_rate": 0.0,
                    "inflow": 0.0,
                    "outflow": 0.0,
                    "net_amount": 0.0,
                    "amount": 0.0,
                    "main_net": 0.0,
                }
                for col in df.columns:
                    col_str = str(col)
                    if "主力" in col_str:
                        row["main_net"] = float(latest[col])
                    elif "超大单" in col_str:
                        row["super_large_net"] = float(latest[col])
                    elif "大单" in col_str:
                        row["large_net"] = float(latest[col])
                all_rows.append(row)
                consecutive_fail = 0
        except Exception:
            consecutive_fail += 1

    return pd.DataFrame(all_rows) if all_rows else pd.DataFrame()


def _get_priority_codes(trade_date: str, db: Storage | None = None) -> list[str]:
    """从 DB 获取当日涨停+龙虎榜的股票代码。"""
    try:
        source_db = db or Storage()
        conn = source_db._get_conn()
        codes = []
        for table in ["zt_pool", "lhb_detail"]:
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT stock_code FROM {table} WHERE trade_date = ?",
                    (trade_date,),
                ).fetchall()
                codes.extend([r[0] for r in rows])
            except Exception:
                pass
        conn.close()
        return list(dict.fromkeys(codes))
    except Exception:
        return []


def save(df: pd.DataFrame, db: Storage, dedup: bool = False) -> int:
    """将资金流向数据写入数据库。

    写入前过滤 DB 不存在的 stock_code（fund_flow 列可能比 daily_price 多）。
    """
    if df.empty:
        return 0

    # 如果有新版列（stock_name, pct_change 等），需要确保 DB 有这些列
    conn = db._get_conn()
    existing_cols = {r[1] for r in conn.execute("PRAGMA table_info([fund_flow])").fetchall()}
    for col in ["stock_name", "pct_change", "turnover_rate", "inflow", "outflow", "net_amount", "amount"]:
        if col not in existing_cols:
            try:
                conn.execute(f"ALTER TABLE fund_flow ADD COLUMN {col} REAL DEFAULT 0" if col != "stock_name" else f"ALTER TABLE fund_flow ADD COLUMN {col} TEXT DEFAULT ''")
                conn.commit()
            except Exception:
                pass
    conn.close()

    return db.insert("fund_flow", df, dedup=dedup)
