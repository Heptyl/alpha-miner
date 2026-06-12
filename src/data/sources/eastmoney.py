"""东财datacenter通用模块 — 统一HTTP请求、分页、防封

东财数据中心(datacenter-web.eastmoney.com)提供:
  - 龙虎榜详情/席位明细
  - 北向资金
  - 解禁日历
  - 股东增减持
  - 大宗交易
  等等

统一接口: datacenter-web.eastmoney.com/api/data/v1/get
参数: reportName(数据集), columns(字段), filter(过滤), sortColumns/sortTypes, pageSize/pageNumber

用法:
    from src.data.sources.eastmoney import EastMoneyClient

    em = EastMoneyClient()
    df = em.fetch_report(
        report_name="RPT_DAILYBILLBOARD_DETAILSNEW",
        columns="SECURITY_CODE,TRADE_DATE,CLOSE_PRICE,CHANGE_RATE",
        filter_expr="(TRADE_DATE>='2024-01-01')",
    )
"""

import logging
import random
import time
from typing import Optional

import pandas as pd
import requests

logger = logging.getLogger(__name__)

# 东财datacenter API基础URL
DC_BASE_URL = "https://datacenter-web.eastmoney.com/api/data/v1/get"

# 东财push2 API基础URL(盘中实时)
PUSH2_BASE_URL = "http://push2.eastmoney.com/api/qt/clist/get"

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/125.0.0.0 Safari/537.36"
    ),
    "Referer": "https://data.eastmoney.com/",
    "Accept": "*/*",
}

# 防封参数
_MIN_INTERVAL = 1.0    # 最小请求间隔(秒)
_RAND_DELAY = 0.5      # 随机延迟上限(秒)
_MAX_RETRIES = 3       # 最大重试次数
_RETRY_BACKOFF = 2.0   # 重试退避基数(秒)
_DEFAULT_PAGE_SIZE = 5000


class EastMoneyClient:
    """东财datacenter统一客户端

    特性:
      - 共享Session(TCP复用)
      - 请求间隔控制(防封)
      - 自动分页
      - 重试+退避
      - DataFrame输出
    """

    def __init__(self, min_interval: float = _MIN_INTERVAL,
                 rand_delay: float = _RAND_DELAY,
                 max_retries: int = _MAX_RETRIES):
        self._session: Optional[requests.Session] = None
        self._last_request_time: float = 0.0
        self._min_interval = min_interval
        self._rand_delay = rand_delay
        self._max_retries = max_retries

    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = requests.Session()
            self._session.headers.update(_HEADERS)
        return self._session

    def _wait(self):
        """防封: 最小间隔 + 随机延迟"""
        now = time.time()
        elapsed = now - self._last_request_time
        wait = self._min_interval - elapsed + random.uniform(0, self._rand_delay)
        if wait > 0:
            time.sleep(wait)
        self._last_request_time = time.time()

    def _request_json(self, url: str, params: dict) -> dict | None:
        """带重试的HTTP GET, 返回JSON dict或None."""
        session = self._get_session()
        for attempt in range(1, self._max_retries + 1):
            self._wait()
            try:
                resp = session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                data = resp.json()
                if data.get("success") is False:
                    logger.warning(f"东财API返回失败: {data.get('message', 'unknown')}")
                    return None
                return data
            except requests.RequestException as e:
                logger.warning(f"东财API请求失败(第{attempt}次): {e}")
                if attempt < self._max_retries:
                    time.sleep(_RETRY_BACKOFF * attempt)
        return None

    def fetch_report(self, *, report_name: str, columns: str = "ALL",
                     filter_expr: str = "",
                     sort_columns: str = "", sort_types: str = "",
                     page_size: int = _DEFAULT_PAGE_SIZE,
                     max_pages: int = 100) -> pd.DataFrame:
        """分页获取东财datacenter报表数据

        Args:
            report_name: 数据集名(如 RPT_DAILYBILLBOARD_DETAILSNEW)
            columns: 字段列表(逗号分隔, "ALL"=全量)
            filter_expr: 过滤表达式(如 "(TRADE_DATE>='2024-01-01')")
            sort_columns: 排序列(逗号分隔)
            sort_types: 排序方向(逗号分隔, 1=升序/-1=降序)
            page_size: 每页条数
            max_pages: 最大页数(防无限循环)

        Returns:
            合并后的DataFrame, 空则返回空DataFrame
        """
        params = {
            "reportName": report_name,
            "columns": columns,
            "pageSize": str(page_size),
            "pageNumber": "1",
            "source": "WEB",
            "client": "WEB",
        }
        if filter_expr:
            params["filter"] = filter_expr
        if sort_columns:
            params["sortColumns"] = sort_columns
        if sort_types:
            params["sortTypes"] = sort_types

        # 第一页: 获取总页数
        data = self._request_json(DC_BASE_URL, params)
        if not data or not data.get("result"):
            return pd.DataFrame()

        result = data["result"]
        total_pages = result.get("pages", 1)
        first_data = result.get("data", [])

        if total_pages <= 1:
            return pd.DataFrame(first_data) if first_data else pd.DataFrame()

        # 后续页
        all_data = list(first_data)
        for page in range(2, min(total_pages + 1, max_pages + 1)):
            params["pageNumber"] = str(page)
            data = self._request_json(DC_BASE_URL, params)
            if not data or not data.get("result"):
                break
            page_data = data["result"].get("data", [])
            if not page_data:
                break
            all_data.extend(page_data)

            if page % 5 == 0:
                logger.debug(f"{report_name} 分页: {page}/{total_pages}")

        logger.info(f"{report_name} 获取完成: {len(all_data)}条, {min(total_pages, max_pages)}页")
        return pd.DataFrame(all_data)

    def fetch_single_page(self, *, report_name: str, columns: str = "ALL",
                          filter_expr: str = "",
                          sort_columns: str = "", sort_types: str = "",
                          page_size: int = _DEFAULT_PAGE_SIZE,
                          page_number: int = 1) -> pd.DataFrame:
        """获取单页数据(不自动翻页)

        适用于已知数据量不大或只需首页的场景。
        """
        params = {
            "reportName": report_name,
            "columns": columns,
            "pageSize": str(page_size),
            "pageNumber": str(page_number),
            "source": "WEB",
            "client": "WEB",
        }
        if filter_expr:
            params["filter"] = filter_expr
        if sort_columns:
            params["sortColumns"] = sort_columns
        if sort_types:
            params["sortTypes"] = sort_types

        data = self._request_json(DC_BASE_URL, params)
        if not data or not data.get("result"):
            return pd.DataFrame()
        return pd.DataFrame(data["result"].get("data", []))

    def close(self):
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


# 模块级单例(便捷访问)
_default_client: Optional[EastMoneyClient] = None


def get_client() -> EastMoneyClient:
    """获取模块级东财客户端单例"""
    global _default_client
    if _default_client is None:
        _default_client = EastMoneyClient()
    return _default_client


# ---------------------------------------------------------------------------
# 便捷函数: 常用数据集
# ---------------------------------------------------------------------------

def fetch_lhb_detail(start_date: str, end_date: str) -> pd.DataFrame:
    """龙虎榜详情

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    """
    return get_client().fetch_report(
        report_name="RPT_DAILYBILLBOARD_DETAILSNEW",
        columns="ALL",
        filter_expr=f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')",
        sort_columns="SECURITY_CODE,TRADE_DATE",
        sort_types="1,-1",
    )


def fetch_lhb_seat_detail(trade_date: str) -> pd.DataFrame:
    """龙虎榜席位买卖明细(个股每日营业部明细)

    Args:
        trade_date: 交易日期 (YYYY-MM-DD)
    """
    return get_client().fetch_report(
        report_name="RPT_BILLBOARD_DAILYDETAILSBUY",
        columns="ALL",
        filter_expr=f"(TRADE_DATE='{trade_date}')",
        sort_columns="SECURITY_CODE",
        sort_types="1",
    )


def fetch_northbound_flow(start_date: str, end_date: str) -> pd.DataFrame:
    """北向资金(沪深港通额度)

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    """
    return get_client().fetch_report(
        report_name="RPT_MUTUAL_QUOTA",
        columns="ALL",
        filter_expr=f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')",
        sort_columns="TRADE_DATE",
        sort_types="-1",
    )


def fetch_lockup_calendar(start_date: str, end_date: str,
                          market: str = "全部股票") -> pd.DataFrame:
    """解禁日历(限售股解禁汇总)

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
        market: 市场筛选 (全部股票/沪市A股/深市A股等)
    """
    market_map = {
        "全部股票": "000300", "沪市A股": "000001", "科创板": "000688",
        "深市A股": "399001", "创业板": "399001", "京市A股": "999999",
    }
    code = market_map.get(market, "000300")
    return get_client().fetch_report(
        report_name="RPT_LIFTDAY_STA",
        columns="ALL",
        filter_expr=f'(INDEX_CODE="{code}")(FREE_DATE>=\'{start_date}\')(FREE_DATE<=\'{end_date}\')',
        sort_columns="FREE_DATE",
        sort_types="1",
    )


def fetch_block_trade(start_date: str, end_date: str) -> pd.DataFrame:
    """大宗交易明细

    Args:
        start_date: 开始日期 (YYYY-MM-DD)
        end_date: 结束日期 (YYYY-MM-DD)
    """
    return get_client().fetch_report(
        report_name="RPT_DATA_BLOCKTRADE",
        columns="ALL",
        filter_expr=f"(TRADE_DATE<='{end_date}')(TRADE_DATE>='{start_date}')",
        sort_columns="TRADE_DATE",
        sort_types="-1",
    )
