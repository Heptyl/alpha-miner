"""资金流向数据采集 — 同花顺全市场排名接口。

主源: 同花顺个股资金流排名 (data.10jqka.com.cn/funds/ggzjl/)
      一次拉全量（~5200只，约110页，1s/页 ≈ 2min）
回退: stock_individual_fund_flow (东方财富逐只，仅WAF严重时降级)

同花顺接口稳定，不走东财WAF，返回：代码、名称、涨跌幅、资金流入/流出/净额、成交额。
无超大单/大单拆分，但有主力净额（净额即主力净流入）。
"""

import json
import logging
import re
import sys
import time
from pathlib import Path

import pandas as pd
import py_mini_racer
import requests
from bs4 import BeautifulSoup

from src.data.storage import Storage

logger = logging.getLogger(__name__)

_THS_JS_PATH = Path(__file__).resolve().parents[3] / ".venv" / "lib" / f"python3.{sys.version_info.minor}" / "site-packages" / "akshare" / "data" / "ths.js"
if not _THS_JS_PATH.exists():
    # Fallback: search for any python3.x in .venv/lib
    _lib_dir = Path(__file__).resolve().parents[3] / ".venv" / "lib"
    _found = list(_lib_dir.glob("python3.*/site-packages/akshare/data/ths.js"))
    _THS_JS_PATH = _found[0] if _found else _THS_JS_PATH

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

_PAGE_SIZE = 50  # 每页50只
_PAGE_DELAY = 0.8  # 每页间隔秒数


def _get_ths_v_code() -> str:
    """获取同花顺 hexin-v 验证码。"""
    js_code = py_mini_racer.MiniRacer()
    with open(_THS_JS_PATH) as f:
        js_code.eval(f.read())
    return js_code.call("v")


def _parse_amount(text: str) -> float:
    """解析同花顺金额字符串（如 '43.72亿', '-1812.83万', '8749.41万'）为万元。"""
    text = text.strip()
    if not text or text == "0.00" or text == "-":
        return 0.0
    m = re.match(r"([+-]?\d+\.?\d*)\s*(亿|万)?", text)
    if not m:
        return 0.0
    val = float(m.group(1))
    unit = m.group(2)
    if unit == "亿":
        return val * 10000  # 转为万元
    return val  # 已经是万元


def _fetch_ths_rank(trade_date: str) -> pd.DataFrame:
    """从同花顺拉全市场个股资金流排名。

    返回 DataFrame，列: stock_code, trade_date, stock_name, pct_change,
    inflow, outflow, net_amount, amount, main_net
    金额单位：万元。
    """
    try:
        v_code = _get_ths_v_code()
    except Exception as e:
        logger.warning("fund_flow: 获取 ths v_code 失败: %s", e)
        return pd.DataFrame()

    headers = {**_HEADERS, "hexin-v": v_code}
    all_rows = []

    # 先请求第1页获取总页数
    url_tpl = (
        "http://data.10jqka.com.cn/funds/ggzjl/"
        "field/zdf/order/desc/page/{page}/ajax/1/free/1/"
    )

    try:
        r = requests.get(url_tpl.format(page=1), headers=headers, timeout=15)
        r.raise_for_status()
    except Exception as e:
        logger.warning("fund_flow ths 第1页请求失败: %s", e)
        return pd.DataFrame()

    soup = BeautifulSoup(r.text, features="lxml")
    table = soup.find("table")
    if not table:
        logger.warning("fund_flow ths: 无数据（可能被反爬）")
        return pd.DataFrame()

    # 获取总页数
    page_info = soup.find("span", attrs={"class": "page_info"})
    if page_info:
        total_pages = int(page_info.text.split("/")[1])
    else:
        total_pages = 110  # 默认估计

    logger.info("fund_flow ths: 共 %d 页", total_pages)
    max_pages = min(total_pages, 120)  # 全市场 ~5200 只（~110 页）

    # 解析第1页
    all_rows.extend(_parse_table(table, trade_date))

    # 拉后续页（定期刷新 v_code 防 401）
    for page in range(2, max_pages + 1):
        time.sleep(_PAGE_DELAY)

        # 每4页刷新一次 v_code
        if page % 4 == 2:
            try:
                v_code = _get_ths_v_code()
                headers = {**_HEADERS, "hexin-v": v_code}
            except Exception:
                pass

        page_retries = 0
        while page_retries < 2:
            try:
                r = requests.get(url_tpl.format(page=page), headers=headers, timeout=15)
                if r.status_code == 401:
                    v_code = _get_ths_v_code()
                    headers = {**_HEADERS, "hexin-v": v_code}
                    time.sleep(1)
                    page_retries += 1
                    continue
                r.raise_for_status()
                soup = BeautifulSoup(r.text, features="lxml")
                table = soup.find("table")
                if not table:
                    logger.warning("fund_flow ths 第%d页无 table", page)
                    break
                rows = _parse_table(table, trade_date)
                if not rows:
                    logger.info("fund_flow ths 第%d页空，停止", page)
                    break
                all_rows.extend(rows)
                break  # 成功，跳出 retry
            except Exception as e:
                page_retries += 1
                if page_retries < 2 and "401" in str(e):
                    try:
                        v_code = _get_ths_v_code()
                        headers = {**_HEADERS, "hexin-v": v_code}
                        time.sleep(1)
                        continue
                    except Exception:
                        pass
                logger.warning("fund_flow ths 第%d页失败: %s", page, e)
                break

        if page % 20 == 0:
            logger.info("fund_flow ths 进度: %d/%d, 累计 %d 只", page, total_pages, len(all_rows))

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


def fetch(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取资金流向。

    数据源优先级：
    1. 同花顺全市场排名（~2min 拉完，最全）
    2. 新浪全市场资金流排名（快速，~5000只）
    3. 东财逐只（只拉重点股票，WAF严重时降级）
    """
    # 主源：同花顺
    result = _fetch_ths_rank(trade_date)
    if not result.empty:
        return result

    # 备选1：新浪
    logger.warning("fund_flow: 同花顺失败，回退新浪")
    result = _fetch_sina_rank(trade_date)
    if not result.empty:
        return result

    # 备选2：东财逐只（可能被 WAF）
    logger.warning("fund_flow: 新浪也失败，回退东财逐只")
    return _fetch_em_fallback(trade_date)


def _fetch_sina_rank(trade_date: str) -> pd.DataFrame:
    """从新浪拉全市场个股资金流排名。

    接口: vip.stock.finance.sina.com.cn MoneyFlow.ssl_bkzj_ssggzj
    逐页拉取，每页50只，全市场约5000只（~100页）。
    注意：只返回当天数据，不能指定历史日期。
    """
    _SINA_URL = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php/MoneyFlow.ssl_bkzj_ssggzj"
    _SINA_PAGE_SIZE = 80
    _SINA_DELAY = 0.5  # 每页间隔

    all_rows = []
    page = 1

    while True:
        try:
            r = requests.get(
                _SINA_URL,
                params={
                    "page": page,
                    "num": _SINA_PAGE_SIZE,
                    "sort": "netamount",
                    "asc": 0,
                    "nodeId": "hs_a",
                },
                timeout=15,
                headers={"User-Agent": "Mozilla/5.0"},
            )
            if r.status_code != 200 or not r.text.strip():
                logger.warning("fund_flow sina 第%d页 HTTP %d 或空响应", page, r.status_code)
                break

            data = json.loads(r.text)
            if not isinstance(data, list) or not data:
                logger.info("fund_flow sina 第%d页无数据，停止", page)
                break

            for d in data:
                symbol = d.get("symbol", "")
                # sh600519 -> 600519, sz000001 -> 000001
                stock_code = symbol[2:] if len(symbol) > 2 else symbol
                if not stock_code or not stock_code.isdigit():
                    continue
                try:
                    all_rows.append({
                        "stock_code": stock_code,
                        "trade_date": trade_date,
                        "stock_name": d.get("name", ""),
                        "pct_change": float(d.get("changeratio", 0)) * 100,
                        "turnover_rate": float(d.get("turnover", 0)),
                        "inflow": float(d.get("inamount", 0)),
                        "outflow": float(d.get("outamount", 0)),
                        "net_amount": float(d.get("netamount", 0)),
                        "amount": float(d.get("amount", 0)),
                        "main_net": float(d.get("r0_net", 0)),  # 主力净流入
                    })
                except (ValueError, TypeError):
                    continue

            if page % 10 == 0:
                logger.info("fund_flow sina 进度: 第%d页, 累计 %d 只", page, len(all_rows))

            # 不足一页说明到头了
            if len(data) < _SINA_PAGE_SIZE:
                break

            page += 1
            time.sleep(_SINA_DELAY)

        except Exception as e:
            logger.warning("fund_flow sina 第%d页失败: %s", page, e)
            # 重试逻辑：前3页失败直接退出，后面的跳过继续
            if page <= 3:
                break
            page += 1
            time.sleep(3)

    if not all_rows:
        return pd.DataFrame()

    logger.info("fund_flow sina 完成: 共 %d 只", len(all_rows))
    return pd.DataFrame(all_rows)


def _fetch_em_fallback(trade_date: str) -> pd.DataFrame:
    """回退：东财逐只资金流（只拉涨停+龙虎榜，限流更严）。"""
    import akshare as ak

    codes = _get_priority_codes(trade_date)
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


def _get_priority_codes(trade_date: str) -> list[str]:
    """从 DB 获取当日涨停+龙虎榜的股票代码。"""
    try:
        db = Storage()
        conn = db._get_conn()
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
