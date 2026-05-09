"""日K线数据采集 — 去掉东方财富 spot_em，用 stock_zh_a_daily 逐只拉取为主源。

数据源优先级：
1. akshare stock_zh_a_daily (新浪源，逐只拉取，稳定)
2. 腾讯行情 qt.gtimg.cn (批量实时行情，兜底)

backfill 模式:
- 同样用 stock_zh_a_daily，多线程加速
- 失败回退腾讯行情（仅当日数据可用）
"""

import logging
import time
from datetime import datetime

import akshare as ak
import pandas as pd
import requests

from src.data.storage import Storage

logger = logging.getLogger(__name__)

# ── 限流 / 退避参数 ──
_BACKOFF_BASE = 2          # 退避基数（秒）
_MAX_BACKOFF = 30          # 最大退避时间
_PER_STOCK_DELAY = 0.15    # 每只股票间最小间隔（秒），避免触发限流
_BATCH_FAILURE_THRESHOLD = 20  # 连续失败多少只后放弃该源

# 腾讯行情字段映射 (qt.gtimg.cn 返回 ~ 分隔的字段)
_TENCENT_FIELD_MAP = {
    3: "close",       # 现价
    4: "pre_close",   # 昨收
    5: "open",        # 今开
    33: "high",       # 最高
    34: "low",        # 最低
    37: "volume",     # 成交量(手)
    38: "turnover_rate",  # 换手率(%)
}

# 股票代码前缀 → 腾讯格式
_PREFIX_MAP = {
    "6": "sh",  # 沪市主板
    "0": "sz",  # 深市主板
    "3": "sz",  # 创业板
    "4": "bj",  # 北交所
    "8": "bj",  # 北交所
}


def _code_to_tencent(code: str) -> str:
    """A 股代码转腾讯格式: 000001 → sz000001."""
    prefix = _PREFIX_MAP.get(code[0], "sz")
    return f"{prefix}{code}"


def _code_from_tencent(tc: str) -> str:
    """腾讯格式转 A 股代码: sz000001 → 000001."""
    return tc[2:]


def _fetch_tencent_batch(codes: list[str], batch_size: int = 800) -> pd.DataFrame:
    """从腾讯行情接口批量获取实时数据。

    qt.gtimg.cn 一次最多约 800 只，超过则分批。
    """
    all_rows = []

    for i in range(0, len(codes), batch_size):
        batch = [_code_to_tencent(c) for c in codes[i:i + batch_size]]
        url = "https://qt.gtimg.cn/q=" + ",".join(batch)

        try:
            r = requests.get(url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
            if r.status_code != 200:
                logger.warning("腾讯行情 HTTP %d", r.status_code)
                continue

            for line in r.text.strip().split(";"):
                line = line.strip()
                if not line or "~" not in line:
                    continue
                parts = line.split("~")
                if len(parts) < 40:
                    continue

                # 提取代码: v_sh600519="1 → 600519
                raw_code = parts[0]
                if "_" in raw_code:
                    tc_code = raw_code.split("_")[1].split("=")[0] if "=" in raw_code else ""
                    stock_code = _code_from_tencent(tc_code) if len(tc_code) > 2 else ""
                else:
                    stock_code = ""

                if not stock_code:
                    continue

                row = {"stock_code": stock_code}
                try:
                    for idx, col in _TENCENT_FIELD_MAP.items():
                        val = parts[idx] if idx < len(parts) else ""
                        row[col] = float(val) if val else None
                except (ValueError, IndexError):
                    continue

                # 成交额: 从 [35] 解析 (1410.89/36451/5127146041)
                try:
                    amt_str = parts[35] if len(parts) > 35 else ""
                    amt_parts = amt_str.split("/")
                    row["amount"] = float(amt_parts[2]) if len(amt_parts) > 2 else None
                except (ValueError, IndexError):
                    row["amount"] = None

                all_rows.append(row)

        except Exception as e:
            logger.warning("腾讯行情批次 %d 失败: %s", i // batch_size, e)
            continue

    if not all_rows:
        return pd.DataFrame()

    return pd.DataFrame(all_rows)


def _fetch_tencent_full(trade_date: str) -> pd.DataFrame:
    """腾讯行情: 优先从 DB 获取代码列表，再批量拉行情."""
    codes = []

    # 优先从 DB 已有表获取代码
    try:
        db = Storage()
        db.init_db()
        conn = db._get_conn()
        codes = [r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price "
            "UNION SELECT DISTINCT stock_code FROM zt_pool "
            "UNION SELECT DISTINCT stock_code FROM lhb_detail "
            "UNION SELECT DISTINCT stock_code FROM strong_pool"
        ).fetchall()]
        conn.close()
        if codes:
            logger.info("从 DB 获取 %d 只股票代码", len(codes))
    except Exception as e:
        logger.warning("DB 获取代码失败: %s", e)

    # 回退: 尝试 akshare stock_info_a_code_name
    if not codes:
        try:
            info_df = ak.stock_info_a_code_name()
            if info_df is not None and not info_df.empty:
                codes = info_df["code"].tolist()
                logger.info("从 stock_info 获取 %d 只 A 股代码", len(codes))
        except Exception as e:
            logger.warning("stock_info_a_code_name 失败: %s", e)

    if not codes:
        logger.error("腾讯回退: 无可用股票代码")
        return pd.DataFrame()

    df = _fetch_tencent_batch(codes)
    if df.empty:
        return pd.DataFrame()

    df["trade_date"] = trade_date
    df = df.dropna(subset=["close"])
    df = df[df["close"] > 0]
    logger.info("腾讯行情获取 %d 只", len(df))
    return df


def _clear_proxy() -> dict:
    """清除代理环境变量（WSL2 Clash 会拦截腾讯/东财国内接口），返回旧值以便恢复。"""
    import os
    old = {}
    for k in ("http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY",
              "all_proxy", "ALL_PROXY"):
        v = os.environ.pop(k, None)
        if v is not None:
            old[k] = v
    if old:
        logger.info("已清除代理变量: %s", list(old.keys()))
    return old


def _restore_proxy(old: dict) -> None:
    """恢复代理环境变量。"""
    import os
    for k, v in old.items():
        os.environ[k] = v


def fetch(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取日K线 — 始终走腾讯全量。

    腾讯行情 qt.gtimg.cn 实测 5200+ 只 / ~1.5s，稳定可靠。
    旧逻辑收盘后只拉重点股票 ~600 只，导致 IC/漂移/进化链路失效，已废弃。

    策略：腾讯全量 → 失败回退 stock_zh_a_daily（逐只，慢但稳）
    """
    old_proxy = _clear_proxy()

    try:
        # 主路径：腾讯全量（~1.5s 拉完 5200+ 只）
        logger.info("fetch(%s): 腾讯全量模式", trade_date)
        result = _fetch_tencent_full(trade_date)
        if not result.empty:
            return result

        # 回退：stock_zh_a_daily 全量（慢，逐只拉取）
        logger.warning("腾讯全量失败，回退 stock_zh_a_daily 全量")
        result = _fetch_daily_ak(trade_date, retries, multi_thread=True)
        if not result.empty:
            return result

        logger.error("所有数据源均失败")
        return pd.DataFrame()
    finally:
        _restore_proxy(old_proxy)


def fetch_today(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取当日实时行情快照 — 只拉重点股票。"""
    return fetch(trade_date, retries=retries)


def fetch_history(trade_date: str, retries: int = 3) -> pd.DataFrame:
    """拉取历史日K线 (backfill) — 优先东财 stock_zh_a_hist，回退 stock_zh_a_daily.

    多线程加速，带限流 + 指数退避 + 连续失败检测。
    """
    # 优先东财源（stock_zh_a_hist，稳定可用）
    result = _fetch_hist_eastmoney(trade_date, retries)
    if not result.empty:
        return result
    # 回退新浪源
    logger.warning("stock_zh_a_hist 全部失败，回退 stock_zh_a_daily")
    return _fetch_daily_ak(trade_date, retries, multi_thread=True)


def _get_priority_codes(trade_date: str) -> list[str]:
    """从 DB 获取当日需要拉取日K线的重点股票代码。

    来源：zt_pool + strong_pool + lhb_detail 涉及的股票
    """
    try:
        db = Storage()
        conn = db._get_conn()
        codes = []
        for table in ["zt_pool", "strong_pool", "lhb_detail"]:
            try:
                rows = conn.execute(
                    f"SELECT DISTINCT stock_code FROM {table} WHERE trade_date = ?",
                    (trade_date,),
                ).fetchall()
                codes.extend([r[0] for r in rows])
            except Exception:
                pass
        conn.close()
        # 去重
        return list(set(codes))
    except Exception as e:
        logger.warning("获取重点股票代码失败: %s", e)
        return []


def _fetch_daily_ak_subset(trade_date: str, codes: list[str], max_retries: int = 3) -> pd.DataFrame:
    """用 stock_zh_a_daily 拉取指定股票列表的日K线（单线程，少量股票用）。

    适用于日常采集只拉几十到几百只重点股票的场景。
    """
    import concurrent.futures

    results = []
    consecutive_fail = 0
    date_str = trade_date.replace("-", "")

    def _fetch_one(code: str) -> dict | None:
        nonlocal consecutive_fail
        time.sleep(_PER_STOCK_DELAY)

        for attempt in range(max_retries):
            try:
                prefix = "sh" if code.startswith("6") else "sz"
                df = ak.stock_zh_a_daily(
                    symbol=f"{prefix}{code}",
                    start_date=date_str,
                    end_date=date_str,
                )
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    consecutive_fail = 0
                    return {
                        "stock_code": code,
                        "trade_date": trade_date,
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0)),
                        "turnover_rate": float(row.get("turnover", 0)),
                    }
                return None
            except Exception as e:
                err_str = str(e).lower()
                is_waf = any(kw in err_str for kw in (
                    "405", "403", "connectionreset", "waf", "forbidden",
                    "blocked", "rate limit", "too many",
                ))
                if is_waf or consecutive_fail >= 5:
                    consecutive_fail += 1
                if attempt < max_retries - 1:
                    time.sleep(min(_BACKOFF_BASE ** (attempt + 1), _MAX_BACKOFF))
                return None
        return None

    # 少量股票用 3 线程足够
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(_fetch_one, c): c for c in codes}
        for future in concurrent.futures.as_completed(futures):
            if consecutive_fail >= _BATCH_FAILURE_THRESHOLD:
                logger.warning("连续失败 %d ≥ %d，终止", consecutive_fail, _BATCH_FAILURE_THRESHOLD)
                for f in futures:
                    f.cancel()
                break
            result = future.result()
            if result:
                results.append(result)

    if results:
        logger.info("stock_zh_a_daily 获取 %d/%d 只 (重点股票 %s)",
                     len(results), len(codes), trade_date)
        return pd.DataFrame(results)

    return pd.DataFrame()


def _get_stock_codes() -> list[str]:
    """获取股票代码列表 — 优先 DB，回退 akshare."""
    codes = []
    try:
        db = Storage()
        db.init_db()
        conn = db._get_conn()
        codes = [r[0] for r in conn.execute(
            "SELECT DISTINCT stock_code FROM daily_price"
        ).fetchall()]
        conn.close()
        if codes:
            logger.info("从 DB 获取 %d 只股票代码", len(codes))
    except Exception as e:
        logger.warning("DB 获取代码失败: %s", e)

    if not codes:
        try:
            info_df = ak.stock_info_a_code_name()
            if info_df is not None and not info_df.empty:
                codes = info_df["code"].tolist()
                logger.info("从 stock_info 获取 %d 只 A 股代码", len(codes))
        except Exception as e:
            logger.warning("stock_info_a_code_name 失败: %s", e)

    return codes


def _fetch_daily_ak(trade_date: str, max_retries: int = 3, multi_thread: bool = False) -> pd.DataFrame:
    """用 stock_zh_a_daily 逐只拉取日K线（新浪源，稳定可用）。

    多线程模式用于 backfill，单线程模式用于日常采集。
    """
    import concurrent.futures
    import threading

    codes = _get_stock_codes()
    if not codes:
        logger.error("_fetch_daily_ak: 无可用股票代码")
        return pd.DataFrame()

    results = []
    consecutive_fail = 0
    total_failed = 0
    lock = threading.Lock()
    backoff_state = {"delay": _PER_STOCK_DELAY, "waf_triggered": False}
    date_str = trade_date.replace("-", "")

    def _fetch_one(code: str) -> dict | None:
        nonlocal consecutive_fail, total_failed

        with lock:
            current_delay = backoff_state["delay"]
            if backoff_state["waf_triggered"] and consecutive_fail >= _BATCH_FAILURE_THRESHOLD:
                return None

        time.sleep(current_delay)

        for attempt in range(max_retries):
            try:
                prefix = "sh" if code.startswith("6") else "sz"
                df = ak.stock_zh_a_daily(
                    symbol=f"{prefix}{code}",
                    start_date=date_str,
                    end_date=date_str,
                )
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    with lock:
                        consecutive_fail = 0
                    return {
                        "stock_code": code,
                        "trade_date": trade_date,
                        "open": float(row.get("open", 0)),
                        "high": float(row.get("high", 0)),
                        "low": float(row.get("low", 0)),
                        "close": float(row.get("close", 0)),
                        "volume": float(row.get("volume", 0)),
                        "amount": float(row.get("amount", 0)),
                        "turnover_rate": float(row.get("turnover", 0)),
                    }
                else:
                    return None
            except Exception as e:
                err_str = str(e).lower()
                is_waf = any(kw in err_str for kw in (
                    "405", "403", "connectionreset", "waf", "forbidden",
                    "blocked", "rate limit", "too many",
                ))
                with lock:
                    consecutive_fail += 1
                    total_failed += 1
                    if is_waf or consecutive_fail >= 5:
                        backoff_state["waf_triggered"] = True
                        new_delay = min(
                            backoff_state["delay"] * _BACKOFF_BASE,
                            _MAX_BACKOFF,
                        )
                        if new_delay != backoff_state["delay"]:
                            logger.warning(
                                "WAF/连续失败: 退避 %.2f→%.2fs (连续失败%d)",
                                backoff_state["delay"], new_delay, consecutive_fail,
                            )
                            backoff_state["delay"] = new_delay

                if attempt < max_retries - 1:
                    sleep_time = min(_BACKOFF_BASE ** (attempt + 1), _MAX_BACKOFF)
                    time.sleep(sleep_time)
                else:
                    return None

        return None

    if multi_thread:
        max_workers = 5
    else:
        max_workers = 3

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_one, c): c for c in codes}
        for future in concurrent.futures.as_completed(futures):
            with lock:
                if consecutive_fail >= _BATCH_FAILURE_THRESHOLD:
                    logger.error(
                        "连续失败 %d ≥ 阈值 %d，终止 stock_zh_a_daily",
                        consecutive_fail, _BATCH_FAILURE_THRESHOLD,
                    )
                    for f in futures:
                        f.cancel()
                    break

            result = future.result()
            if result:
                results.append(result)

    if results:
        logger.info("stock_zh_a_daily 获取 %d/%d 只 (目标 %s)",
                     len(results), len(codes), trade_date)
        return pd.DataFrame(results)

    logger.warning("stock_zh_a_daily 全部失败 (%d 只), 目标 %s", total_failed, trade_date)
    return pd.DataFrame()


def _fetch_hist_eastmoney(trade_date: str, max_retries: int = 3) -> pd.DataFrame:
    """用 stock_zh_a_hist（东方财富源）逐只拉取日K线。

    东财源比新浪源稳定，在国内网络环境下可用性更高。
    多线程加速，带限流 + 连续失败检测。
    """
    import concurrent.futures
    import threading

    codes = _get_stock_codes()
    if not codes:
        logger.error("_fetch_hist_eastmoney: 无可用股票代码")
        return pd.DataFrame()

    results = []
    consecutive_fail = 0
    total_failed = 0
    lock = threading.Lock()
    date_str = trade_date.replace("-", "")
    per_delay = 0.1  # 东财源限流较宽松

    def _fetch_one(code: str) -> dict | None:
        nonlocal consecutive_fail, total_failed

        with lock:
            if consecutive_fail >= _BATCH_FAILURE_THRESHOLD:
                return None

        time.sleep(per_delay)

        for attempt in range(max_retries):
            try:
                df = ak.stock_zh_a_hist(
                    symbol=code,
                    period="daily",
                    start_date=date_str,
                    end_date=date_str,
                    adjust="qfq",
                )
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    with lock:
                        consecutive_fail = 0
                    return {
                        "stock_code": code,
                        "trade_date": trade_date,
                        "open": float(row.get("开盘", 0) or 0),
                        "high": float(row.get("最高", 0) or 0),
                        "low": float(row.get("最低", 0) or 0),
                        "close": float(row.get("收盘", 0) or 0),
                        "volume": float(row.get("成交量", 0) or 0),
                        "amount": float(row.get("成交额", 0) or 0),
                        "turnover_rate": float(row.get("换手率", 0) or 0),
                    }
                return None
            except Exception as e:
                with lock:
                    consecutive_fail += 1
                    total_failed += 1
                err_str = str(e).lower()
                if any(kw in err_str for kw in ("403", "405", "forbidden", "waf", "rate limit")):
                    time.sleep(min(2 ** (attempt + 1), 10))
                else:
                    return None

        return None

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        futures = {executor.submit(_fetch_one, c): c for c in codes}
        for future in concurrent.futures.as_completed(futures):
            with lock:
                if consecutive_fail >= _BATCH_FAILURE_THRESHOLD:
                    logger.error(
                        "连续失败 %d ≥ 阈值 %d，终止 stock_zh_a_hist",
                        consecutive_fail, _BATCH_FAILURE_THRESHOLD,
                    )
                    for f in futures:
                        f.cancel()
                    break
            result = future.result()
            if result:
                results.append(result)

    if results:
        logger.info("stock_zh_a_hist 获取 %d/%d 只 (目标 %s)",
                     len(results), len(codes), trade_date)
        return pd.DataFrame(results)

    logger.warning("stock_zh_a_hist 全部失败 (%d 只), 目标 %s", total_failed, trade_date)
    return pd.DataFrame()


def save(df: pd.DataFrame, db: Storage, dedup: bool = False) -> int:
    """将行情数据写入数据库。"""
    if df.empty:
        return 0
    return db.insert("daily_price", df, dedup=dedup)
