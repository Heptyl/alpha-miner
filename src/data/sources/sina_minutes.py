"""Strict direct Sina RAW 5-minute K-line adapter."""

from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

import requests

SINA_MINUTE_URL = (
    "https://quotes.sina.cn/cn/api/openapi.php/"
    "CN_MarketDataService.getKLineData"
)
SOURCE_NAME = "sina_cn_marketdata"
PERIOD = "5m"
ADJUST = "RAW"
DATALEN = 1970
SHANGHAI = ZoneInfo("Asia/Shanghai")


class SinaMinuteError(RuntimeError):
    """A source failure with bounded-retry and circuit-breaker semantics."""

    def __init__(
        self,
        message: str,
        *,
        retryable: bool = False,
        circuit_breaker: bool = False,
        status_code: int | None = None,
    ) -> None:
        super().__init__(message)
        self.retryable = retryable
        self.circuit_breaker = circuit_breaker
        self.status_code = status_code


@dataclass(frozen=True)
class MinuteBar:
    stock_code: str
    bar_time: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float
    source: str = SOURCE_NAME
    period: str = PERIOD
    adjust: str = ADJUST


def fetch_raw_5m(
    stock_code: str,
    *,
    session: requests.Session | None = None,
    timeout: float = 20,
) -> list[MinuteBar]:
    """Fetch up to 1970 unadjusted five-minute bars with one normal request."""
    symbol = market_symbol(stock_code)
    owns_session = session is None
    client = session or requests.Session()
    try:
        try:
            response = client.get(
                SINA_MINUTE_URL,
                params={
                    "symbol": symbol,
                    "scale": "5",
                    "ma": "no",
                    "datalen": str(DATALEN),
                },
                headers={"Referer": "https://finance.sina.com.cn/"},
                timeout=timeout,
            )
        except requests.Timeout as exc:
            raise SinaMinuteError("Sina 5分钟请求超时", retryable=True) from exc
        except requests.RequestException as exc:
            raise SinaMinuteError(
                f"Sina 5分钟请求失败：{type(exc).__name__}"
            ) from exc

        status_code = int(response.status_code)
        if status_code == 403:
            raise SinaMinuteError(
                "Sina返回403，立即熔断",
                circuit_breaker=True,
                status_code=status_code,
            )
        if status_code == 429 or 500 <= status_code <= 599:
            raise SinaMinuteError(
                f"Sina返回可重试HTTP {status_code}",
                retryable=True,
                status_code=status_code,
            )
        if status_code < 200 or status_code >= 300:
            raise SinaMinuteError(
                f"Sina返回HTTP {status_code}", status_code=status_code
            )

        payload = _decode_payload(response.text)
        raw_rows = _extract_rows(payload)
        bars = [_normalize_bar(stock_code, row, index) for index, row in enumerate(raw_rows, 1)]
        if not bars:
            raise SinaMinuteError("Sina 5分钟数据为空")
        bar_times = [bar.bar_time for bar in bars]
        if len(bar_times) != len(set(bar_times)):
            raise SinaMinuteError("Sina 5分钟数据包含重复bar_time")
        return sorted(bars, key=lambda bar: bar.bar_time)
    finally:
        if owns_session:
            client.close()


def market_symbol(stock_code: str) -> str:
    """Map a strict six-digit mainland code to Sina's explicit market prefix."""
    code = str(stock_code).strip()
    if not re.fullmatch(r"\d{6}", code):
        raise SinaMinuteError("股票代码必须为6位数字")
    if code.startswith(("60", "68")):
        return f"sh{code}"
    if code.startswith(("00", "30")):
        return f"sz{code}"
    if code.startswith(("4", "8", "9")):
        return f"bj{code}"
    raise SinaMinuteError(f"无法确定股票市场前缀：{code}")


def _decode_payload(text: str) -> Any:
    value = str(text or "").strip()
    if not value:
        raise SinaMinuteError("Sina 5分钟响应为空")
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        match = re.fullmatch(r"[A-Za-z_$][\w.$]*\s*\((.*)\)\s*;?", value, re.DOTALL)
        if not match:
            raise SinaMinuteError("Sina 5分钟响应不是合法JSON或JSONP")
        try:
            return json.loads(match.group(1))
        except json.JSONDecodeError as exc:
            raise SinaMinuteError("Sina 5分钟JSONP内容异常") from exc


def _extract_rows(payload: Any) -> list[dict[str, Any]]:
    if not isinstance(payload, dict):
        raise SinaMinuteError("Sina 5分钟响应根节点不是对象")
    result = payload.get("result")
    if not isinstance(result, dict):
        raise SinaMinuteError("Sina 5分钟响应缺少result")
    status = result.get("status")
    if not isinstance(status, dict) or str(status.get("code")) != "0":
        raise SinaMinuteError("Sina 5分钟业务状态异常")
    rows = result.get("data")
    if not isinstance(rows, list):
        raise SinaMinuteError("Sina 5分钟data不是列表")
    if any(not isinstance(row, dict) for row in rows):
        raise SinaMinuteError("Sina 5分钟data包含非对象行")
    return rows


def _normalize_bar(stock_code: str, raw: dict[str, Any], index: int) -> MinuteBar:
    value = str(raw.get("day") or "").strip()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%d %H:%M:%S").replace(
            tzinfo=SHANGHAI
        )
    except ValueError as exc:
        raise SinaMinuteError(f"Sina第{index}根bar时间异常") from exc
    if parsed.second != 0 or parsed.minute % 5 != 0:
        raise SinaMinuteError(f"Sina第{index}根bar不是5分钟边界")

    open_price = _finite_number(raw.get("open"), "open", index)
    high = _finite_number(raw.get("high"), "high", index)
    low = _finite_number(raw.get("low"), "low", index)
    close = _finite_number(raw.get("close"), "close", index)
    volume = _finite_number(raw.get("volume"), "volume", index)
    amount = _finite_number(raw.get("amount"), "amount", index)
    if high < max(open_price, close, low) or low > min(open_price, close, high):
        raise SinaMinuteError(f"Sina第{index}根bar的OHLC关系异常")
    return MinuteBar(
        stock_code=stock_code,
        bar_time=parsed.isoformat(timespec="seconds"),
        open=open_price,
        high=high,
        low=low,
        close=close,
        volume=volume,
        amount=amount,
    )


def _finite_number(value: Any, field: str, index: int) -> float:
    try:
        parsed = float(value)
    except (TypeError, ValueError) as exc:
        raise SinaMinuteError(f"Sina第{index}根bar的{field}不是数值") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise SinaMinuteError(f"Sina第{index}根bar的{field}不是非负有限数")
    return parsed
