"""Bounded, paginated Sina A-share spot adapter for pre-limit evidence."""

from __future__ import annotations

import math
import re
from typing import Any

import requests

SINA_API_ROOT = "https://vip.stock.finance.sina.com.cn/quotes_service/api/json_v2.php"
SINA_SPOT_URL = f"{SINA_API_ROOT}/Market_Center.getHQNodeData"
SINA_COUNT_URL = f"{SINA_API_ROOT}/Market_Center.getHQNodeStockCount"
SOURCE_NAME = "sina_market_center_hs_a"
PAGE_SIZE = 100
MAX_PAGES = 80


class SinaPrelimitError(RuntimeError):
    """Raised when the paginated market response is unusable as evidence."""


def fetch_all_spot(
    session: requests.Session | None = None,
    timeout: float = 15,
) -> list[dict[str, Any]]:
    """Fetch and strictly normalize all rows declared by Sina's market count."""
    owns_session = session is None
    client = session or requests.Session()
    try:
        expected_count = _fetch_expected_count(client, timeout)
        page_count = math.ceil(expected_count / PAGE_SIZE)
        if page_count < 1 or page_count > MAX_PAGES:
            raise SinaPrelimitError(
                f"Sina市场页数异常：{page_count}（上限{MAX_PAGES}）"
            )

        normalized_by_code: dict[str, dict[str, Any]] = {}
        page_signatures: set[tuple[str, ...]] = set()
        received_count = 0
        for page in range(1, page_count + 1):
            payload = _fetch_page(client, page, timeout)
            signature = tuple(str(row.get("code") or "") for row in payload)
            if signature in page_signatures:
                raise SinaPrelimitError(f"Sina第{page}页与此前页面重复")
            page_signatures.add(signature)
            received_count += len(payload)
            for index, raw in enumerate(payload, 1):
                if not isinstance(raw, dict):
                    raise SinaPrelimitError(f"Sina第{page}页第{index}行不是对象")
                try:
                    normalized = _normalize_row(raw)
                except (TypeError, ValueError) as exc:
                    raise SinaPrelimitError(
                        f"Sina第{page}页第{index}行字段异常：{exc}"
                    ) from exc
                # The endpoint can overlap a boundary while its market count changes.
                # Merge by code so callers receive a deterministic unique universe.
                normalized_by_code[normalized["stock_code"]] = normalized

        if received_count < expected_count:
            raise SinaPrelimitError(
                f"Sina分页不完整：声明{expected_count}行，仅收到{received_count}行"
            )
        if not normalized_by_code:
            raise SinaPrelimitError("Sina全市场分页结果为空")
        return [normalized_by_code[code] for code in sorted(normalized_by_code)]
    finally:
        if owns_session:
            client.close()


def _fetch_expected_count(client: requests.Session, timeout: float) -> int:
    payload = _request_json(
        client,
        SINA_COUNT_URL,
        params={"node": "hs_a"},
        timeout=timeout,
        label="市场总数",
    )
    try:
        count = int(payload)
    except (TypeError, ValueError) as exc:
        raise SinaPrelimitError("Sina市场总数结构异常") from exc
    if count <= 0:
        raise SinaPrelimitError("Sina市场总数为空或无效")
    return count


def _fetch_page(
    client: requests.Session,
    page: int,
    timeout: float,
) -> list[dict[str, Any]]:
    payload = _request_json(
        client,
        SINA_SPOT_URL,
        params={
            "page": str(page),
            "num": str(PAGE_SIZE),
            "sort": "symbol",
            "asc": "1",
            "node": "hs_a",
            "symbol": "",
            "_s_r_a": "page",
        },
        timeout=timeout,
        label=f"第{page}页",
    )
    if not isinstance(payload, list):
        raise SinaPrelimitError(f"Sina第{page}页结构异常：应为列表")
    if not payload:
        raise SinaPrelimitError(f"Sina第{page}页为空，禁止接受不完整市场")
    if any(not isinstance(row, dict) for row in payload):
        raise SinaPrelimitError(f"Sina第{page}页结构异常：包含非对象行")
    return payload


def _request_json(
    client: requests.Session,
    url: str,
    *,
    params: dict[str, str],
    timeout: float,
    label: str,
) -> Any:
    try:
        response = client.get(
            url,
            params=params,
            headers={"Referer": "https://finance.sina.com.cn/"},
            timeout=timeout,
        )
        response.raise_for_status()
        return response.json()
    except (requests.RequestException, ValueError) as exc:
        raise SinaPrelimitError(
            f"Sina{label}请求失败：{type(exc).__name__}"
        ) from exc


def _normalize_row(raw: dict[str, Any]) -> dict[str, Any]:
    code = str(raw.get("code") or "").strip()
    if not re.fullmatch(r"\d{6}", code):
        raise ValueError("code必须为6位数字")
    name = str(raw.get("name") or "").strip()
    if not name:
        raise ValueError("name缺失")
    source_time = str(raw.get("ticktime") or "").strip()
    if not re.fullmatch(r"\d{2}:\d{2}(?::\d{2})?", source_time):
        raise ValueError("ticktime缺失或格式错误")
    return {
        "stock_code": code,
        "stock_name": name,
        "price": _required_number(raw.get("trade"), "trade"),
        "open": _required_number(raw.get("open"), "open"),
        "high": _required_number(raw.get("high"), "high"),
        "low": _required_number(raw.get("low"), "low"),
        "volume": _required_number(raw.get("volume"), "volume"),
        "amount": _required_number(raw.get("amount"), "amount"),
        "bid1": _optional_number(raw.get("buy"), "buy"),
        "ask1": _optional_number(raw.get("sell"), "sell"),
        "source_time": source_time,
        "source": SOURCE_NAME,
    }


def _required_number(value: Any, field: str) -> float:
    parsed = _optional_number(value, field)
    if parsed is None:
        raise ValueError(f"{field}缺失")
    return parsed


def _optional_number(value: Any, field: str) -> float | None:
    if value in (None, "", "--"):
        return None
    try:
        parsed = float(str(value).replace(",", ""))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field}不是数值") from exc
    if not math.isfinite(parsed) or parsed < 0:
        raise ValueError(f"{field}不是非负有限数")
    return parsed
