"""基本面数据批量采集 — 现金流量表 + 资产负债表 + 行业分类映射

数据源: akshare (新浪财经 + 东方财富)
采集频率: 一次性全量 / 增量补充
断点续采: data/collect_progress.json
"""
import json
import logging
import sqlite3
import time
from datetime import datetime
from pathlib import Path

import akshare as ak
import pandas as pd

logger = logging.getLogger(__name__)

DB_PATH = "data/alpha_miner.db"
PROGRESS_FILE = Path("data/collect_progress.json")

EXCLUDE_PREFIXES = ("688", "8", "9")  # 科创板/北交所/退市


def _get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def _load_progress() -> dict:
    if PROGRESS_FILE.exists():
        return json.loads(PROGRESS_FILE.read_text())
    return {"cash_flow": [], "balance_sheet": [], "industry_mapping": False}


def _save_progress(progress: dict):
    PROGRESS_FILE.parent.mkdir(parents=True, exist_ok=True)
    PROGRESS_FILE.write_text(json.dumps(progress, ensure_ascii=False))


def _get_stock_list_from_db() -> list[str]:
    """从daily_price表获取已采集的股票列表。"""
    conn = _get_conn()
    rows = conn.execute("""
        SELECT DISTINCT stock_code FROM daily_price
        WHERE stock_code NOT LIKE '688%'
          AND stock_code NOT LIKE '8%'
          AND stock_code NOT LIKE '9%'
        ORDER BY stock_code
    """).fetchall()
    conn.close()
    codes = [r[0] for r in rows]
    logger.info("从DB获取股票列表: %d只", len(codes))
    return codes


def _safe_float(val) -> float | None:
    if val is None or val == "" or val == "--":
        return None
    try:
        f = float(val)
        return f if pd.notna(f) else None
    except (ValueError, TypeError):
        return None


# ============================================================
# 现金流量表 (新浪)
# ============================================================
def _collect_cash_flow(code: str, conn: sqlite3.Connection) -> bool:
    try:
        df = ak.stock_financial_report_sina(stock=code, symbol="现金流量表")
        if df is None or df.empty:
            return False
        row = df.iloc[0]
        report_date = str(row.get("报告日", ""))[:10] or str(row.get("报告日", ""))
        if not report_date or len(report_date) < 8:
            return False
        # 统一为 YYYY-MM-DD 格式
        if len(report_date) == 8:
            report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}"
        conn.execute("""
            INSERT OR REPLACE INTO cash_flow_stmt
            (stock_code, report_date, operate_cash_flow, invest_cash_flow,
             finance_cash_flow, free_cash_flow, cash_change)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            code,
            report_date,
            _safe_float(row.get("经营活动产生的现金流量净额")),
            _safe_float(row.get("投资活动产生的现金流量净额")),
            _safe_float(row.get("筹资活动产生的现金流量净额")),
            _safe_float(row.get("现金及现金等价物净增加额")),
            _safe_float(row.get("现金及现金等价物净增加额")),
        ))
        return True
    except Exception as e:
        logger.debug("现金流量表采集失败 %s: %s", code, e)
        return False


# ============================================================
# 资产负债表 (新浪)
# ============================================================
def _collect_balance_sheet(code: str, conn: sqlite3.Connection) -> bool:
    try:
        df = ak.stock_financial_report_sina(stock=code, symbol="资产负债表")
        if df is None or df.empty:
            return False
        row = df.iloc[0]
        report_date = str(row.get("报告日", ""))[:10] or str(row.get("报告日", ""))
        if not report_date or len(report_date) < 8:
            return False
        if len(report_date) == 8:
            report_date = f"{report_date[:4]}-{report_date[4:6]}-{report_date[6:8]}"
        conn.execute("""
            INSERT OR REPLACE INTO balance_sheet
            (stock_code, report_date, total_assets, total_liabilities,
             total_equity, current_assets, current_liabilities,
             cash_and_equiv, accounts_receivable, inventory, goodwill)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            code,
            report_date,
            _safe_float(row.get("资产总计")),
            _safe_float(row.get("负债合计")),
            _safe_float(row.get("所有者权益合计")),
            _safe_float(row.get("流动资产合计")),
            _safe_float(row.get("流动负债合计")),
            _safe_float(row.get("货币资金")),
            _safe_float(row.get("应收账款")),
            _safe_float(row.get("存货")),
            _safe_float(row.get("商誉")),
        ))
        return True
    except Exception as e:
        logger.debug("资产负债表采集失败 %s: %s", code, e)
        return False


# ============================================================
# 行业分类映射 (东财)
# ============================================================
def _collect_industry_mapping(conn: sqlite3.Connection) -> int:
    try:
        boards_df = ak.stock_board_industry_name_em()
        if boards_df is None or boards_df.empty:
            logger.error("行业板块名称获取失败")
            return 0

        total = 0
        now = datetime.now().strftime("%Y-%m-%d")
        for _, board in boards_df.iterrows():
            board_name = str(board.get("板块名称", ""))
            if not board_name:
                continue
            try:
                members_df = ak.stock_board_industry_cons_em(symbol=board_name)
                if members_df is None or members_df.empty:
                    continue
                for _, m in members_df.iterrows():
                    code = str(m.get("代码", ""))
                    if not code:
                        continue
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_industry_mapping
                        (stock_code, industry_code, industry_name, update_date)
                        VALUES (?, ?, ?, ?)
                    """, (code, board_name, board_name, now))
                    total += 1
                time.sleep(2)
            except Exception as e:
                logger.debug("行业 %s 成分股获取失败: %s", board_name, e)
                time.sleep(5)  # 失败额外等待
                continue

        conn.commit()
        logger.info("行业映射写入: %d条", total)
        return total
    except Exception as e:
        logger.error("行业映射采集失败: %s", e)
        return 0


# ============================================================
# 批量采集入口
# ============================================================
def collect_financial_data(stock_list: list[str] | None = None):
    """批量采集财务数据（现金流量表+资产负债表+行业映射）。

    Args:
        stock_list: 可选指定股票列表，None则从DB获取全量
    """
    progress = _load_progress()
    conn = _get_conn()

    try:
        if stock_list is None:
            codes = _get_stock_list_from_db()
        else:
            codes = stock_list
        if not codes:
            logger.error("股票列表为空，退出")
            return

        completed_cf = set(progress.get("cash_flow", []))
        completed_bs = set(progress.get("balance_sheet", []))
        ok_cf = ok_bs = err = 0

        for i, code in enumerate(codes):
            need_cf = code not in completed_cf
            need_bs = code not in completed_bs

            if not need_cf and not need_bs:
                continue

            cf_ok = _collect_cash_flow(code, conn) if need_cf else True
            bs_ok = _collect_balance_sheet(code, conn) if need_bs else True

            if need_cf:
                if cf_ok:
                    ok_cf += 1
                    progress.setdefault("cash_flow", []).append(code)
                else:
                    err += 1
                    time.sleep(5)  # 失败额外等待

            if need_bs:
                if bs_ok:
                    ok_bs += 1
                    progress.setdefault("balance_sheet", []).append(code)
                else:
                    err += 1
                    time.sleep(5)  # 失败额外等待

            if (i + 1) % 50 == 0:
                conn.commit()
                _save_progress(progress)
                logger.info("进度 %d/%d: 现金流+%d 资产负债+%d 失败%d",
                            i + 1, len(codes), ok_cf, ok_bs, err)

            time.sleep(2)

        conn.commit()
        _save_progress(progress)
        logger.info("财务数据采集完成: 现金流+%d 资产负债+%d 失败%d", ok_cf, ok_bs, err)

        # 行业映射
        if not progress.get("industry_mapping"):
            count = _collect_industry_mapping(conn)
            if count > 0:
                progress["industry_mapping"] = True
                _save_progress(progress)
        else:
            logger.info("行业映射已采集，跳过")

    finally:
        conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    collect_financial_data()
