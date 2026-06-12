"""华泰QMT/xtquant交易网关 — 封装xtquant实现自动下单。

核心功能:
1. 连接华泰QMT客户端(通过xtquant.xttrader)
2. 查询持仓/余额/委托
3. 买入/卖出/撤单
4. 订单状态跟踪(SQLite持久化)
5. 安全限制(日交易次数/单笔金额/股票代码过滤)

注意:
- xtquant只能在Windows上运行(连接本地QMT客户端)
- WSL/Linux环境下自动启用mock模式进行开发调试
- 实盘交易需在Windows环境运行
"""

from __future__ import annotations

import json
import logging
import platform
import sqlite3
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, date
from enum import Enum
from pathlib import Path
from typing import Optional, Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# 常量
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
CONFIG_PATH = PROJECT_ROOT / "config" / "trader.json"
DEFAULT_DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"

# xtquant订单状态映射
class OrderStatus(str, Enum):
    """订单状态枚举。"""
    PENDING = "pending"           # 待报
    SUBMITTED = "submitted"       # 已报
    PARTIAL_FILLED = "partial"    # 部成
    FILLED = "filled"             # 全成
    CANCELLED = "cancelled"       # 已撤
    REJECTED = "rejected"         # 废单
    UNKNOWN = "unknown"           # 未知


# xtquant原始状态码 → OrderStatus映射
XT_ORDER_STATUS_MAP: dict[int, OrderStatus] = {
    48: OrderStatus.PENDING,
    49: OrderStatus.SUBMITTED,
    50: OrderStatus.PARTIAL_FILLED,
    51: OrderStatus.FILLED,
    52: OrderStatus.CANCELLED,
    53: OrderStatus.REJECTED,
}


# ---------------------------------------------------------------------------
# 数据类
# ---------------------------------------------------------------------------

@dataclass
class Position:
    """持仓信息。"""
    stock_code: str          # 证券代码 如 "000001"
    stock_name: str = ""     # 证券名称
    volume: int = 0          # 持仓数量
    available: int = 0       # 可卖数量
    cost_price: float = 0.0  # 成本价
    market_value: float = 0.0  # 市值
    profit_loss: float = 0.0   # 浮动盈亏


@dataclass
class Balance:
    """账户资金信息。"""
    total_asset: float = 0.0     # 总资产
    cash: float = 0.0            # 可用资金
    market_value: float = 0.0    # 持仓市值
    frozen_cash: float = 0.0     # 冻结资金


@dataclass
class OrderRecord:
    """委托订单记录。"""
    order_id: str               # 系统订单号
    stock_code: str             # 证券代码
    direction: str              # "buy" / "sell"
    price: float                # 委托价格
    amount: int                 # 委托数量
    filled_price: float = 0.0   # 成交均价
    filled_amount: int = 0      # 成交数量
    status: str = "pending"     # 订单状态
    order_time: str = ""        # 委托时间
    message: str = ""           # 状态消息
    strategy: str = ""          # 策略标签

    def to_dict(self) -> dict:
        return asdict(self)


# ---------------------------------------------------------------------------
# 安全检查异常
# ---------------------------------------------------------------------------

class TradeSafetyError(Exception):
    """交易安全检查未通过。"""
    pass


class ConnectionError(Exception):
    """QMT连接异常。"""
    pass


# ---------------------------------------------------------------------------
# 数据库表初始化
# ---------------------------------------------------------------------------

_ORDERS_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS trader_orders (
    order_id      TEXT PRIMARY KEY,
    stock_code    TEXT NOT NULL,
    direction     TEXT NOT NULL,         -- buy / sell
    price         REAL NOT NULL,
    amount        INTEGER NOT NULL,
    filled_price  REAL DEFAULT 0,
    filled_amount INTEGER DEFAULT 0,
    status        TEXT DEFAULT 'pending',
    order_time    TEXT DEFAULT '',
    message       TEXT DEFAULT '',
    strategy      TEXT DEFAULT '',
    created_at    TEXT DEFAULT (datetime('now', 'localtime')),
    updated_at    TEXT DEFAULT (datetime('now', 'localtime'))
);
"""

_DAILY_COUNT_INDEX = """
CREATE INDEX IF NOT EXISTS idx_orders_created_at
ON trader_orders(created_at);
"""


def _init_orders_table(db_path: Path) -> None:
    """确保trader_orders表存在。"""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), timeout=30)
    try:
        conn.executescript(_ORDERS_TABLE_DDL + _DAILY_COUNT_INDEX)
        conn.commit()
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# 配置加载
# ---------------------------------------------------------------------------

def load_trader_config(config_path: Path = CONFIG_PATH) -> dict[str, Any]:
    """加载交易配置文件。

    Returns:
        配置字典，含 qmt_path / account_id / max_daily_trades 等字段。
    """
    if not config_path.exists():
        logger.warning("交易配置文件不存在: %s，使用默认值", config_path)
        return {
            "qmt_path": "",
            "account_id": "",
            "account_type": "STOCK",
            "max_daily_trades": 20,
            "max_single_amount": 50000,
            "allowed_prefixes": ["00", "30", "60"],
            "mock_mode": True,
        }
    with open(config_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg


# ---------------------------------------------------------------------------
# Mock xtquant（WSL/Linux开发用）
# ---------------------------------------------------------------------------

class _MockXtQuant:
    """模拟xtquant接口，用于非Windows环境开发调试。

    所有接口返回合理的假数据，订单状态自动流转。
    """

    def __init__(self) -> None:
        self._connected = False
        self._next_order_id = 100000
        self._orders: dict[int, dict] = {}
        self._positions: list[dict] = [
            {"stock_code": "000001", "stock_name": "平安银行",
             "volume": 1000, "available": 1000, "cost_price": 12.50},
            {"stock_code": "600036", "stock_name": "招商银行",
             "volume": 500, "available": 500, "cost_price": 35.20},
        ]
        self._balance = {
            "total_asset": 150000.0, "cash": 50000.0,
            "market_value": 100000.0, "frozen_cash": 0.0,
        }
        logger.info("[Mock] 模拟xtquant已初始化")

    def create_session(self, path: str) -> bool:
        self._connected = True
        logger.info("[Mock] 模拟连接成功: path=%s", path)
        return True

    def get_asset(self, account_id: str) -> Optional[dict]:
        if not self._connected:
            return None
        return dict(self._balance)

    def get_stock_position(self, account_id: str, code: str = "") -> Optional[list[dict]]:
        if not self._connected:
            return None
        if code:
            return [p for p in self._positions if p["stock_code"] == code]
        return list(self._positions)

    def get_orders(self, account_id: str) -> dict[int, dict]:
        return dict(self._orders)

    def order_stock(
        self, account_id: str, code: str, price_type: int,
        price: float, amount: int, direction: int, strategy: str = "",
    ) -> int:
        self._next_order_id += 1
        oid = self._next_order_id
        self._orders[oid] = {
            "order_id": oid,
            "stock_code": code,
            "price": price,
            "amount": amount,
            "direction": direction,
            "status": 51,  # 模拟立即成交
            "filled_price": price,
            "filled_amount": amount,
            "order_time": datetime.now().strftime("%Y%m%d%H%M%S"),
        }
        logger.info("[Mock] 下单成功: order_id=%d code=%s price=%.2f amount=%d",
                     oid, code, price, amount)
        return oid

    def cancel_order(self, account_id: str, order_id: int) -> int:
        if order_id in self._orders:
            self._orders[order_id]["status"] = 52  # 已撤
            logger.info("[Mock] 撤单成功: order_id=%d", order_id)
            return 0
        return -1

    def disconnect(self) -> None:
        self._connected = False
        logger.info("[Mock] 模拟断开连接")


# ---------------------------------------------------------------------------
# 主网关类
# ---------------------------------------------------------------------------

class XtGateway:
    """华泰QMT交易网关。

    封装xtquant库，提供统一的交易接口:
    - connect() / disconnect()  连接管理
    - get_positions() / get_balance() / get_orders()  账户查询
    - buy() / sell() / cancel_order()  交易操作

    内置安全机制:
    - 每日最大交易次数限制
    - 单笔最大金额限制
    - 只允许沪深A股(排除基金/债券/科创板/北交所)
    - 下单前自动检查连接状态

    使用示例::

        gateway = XtGateway()
        gateway.connect()
        balance = gateway.get_balance()
        order_id = gateway.buy("000001", 13.50, 100)
        gateway.disconnect()
    """

    # xtquant价格类型常量
    PRICE_FIXED = 11         # 限价单
    PRICE_MARKET = 5         # 市价单

    # xtquant方向常量
    DIR_BUY = 23
    DIR_SELL = 24

    def __init__(
        self,
        config_path: Path = CONFIG_PATH,
        db_path: Path = DEFAULT_DB_PATH,
    ) -> None:
        """初始化交易网关。

        Args:
            config_path: 交易配置文件路径，默认 config/trader.json
            db_path: SQLite数据库路径，默认 data/alpha_miner.db
        """
        self._config = load_trader_config(config_path)
        self._db_path = db_path
        self._connected = False
        self._xt: Any = None               # xtquant.XtQuantTrader 实例
        self._session_id: int = 0
        self._mock_mode: bool = self._should_use_mock()

        # 安全参数
        self._max_daily_trades: int = self._config.get("max_daily_trades", 20)
        self._max_single_amount: float = float(self._config.get("max_single_amount", 50000))
        self._allowed_prefixes: list[str] = self._config.get("allowed_prefixes", ["00", "30", "60"])

        # 初始化数据库表
        _init_orders_table(db_path)

        mode_label = "MOCK(模拟)" if self._mock_mode else "LIVE(实盘)"
        logger.info("XtGateway 初始化完成 [%s]", mode_label)

    # -------------------------------------------------------------------
    # 私有方法
    # -------------------------------------------------------------------

    def _should_use_mock(self) -> bool:
        """判断是否使用mock模式。

        以下情况启用mock:
        1. 配置文件中 mock_mode=True
        2. 非Windows操作系统(WSL/Linux/macOS)
        3. xtquant库不可导入
        """
        if self._config.get("mock_mode", False):
            return True
        if platform.system() != "Windows":
            logger.info("非Windows系统，自动启用mock模式 (当前: %s)", platform.system())
            return True
        try:
            import xtquant  # noqa: F401
            return False
        except ImportError:
            logger.info("xtquant库不可导入，启用mock模式")
            return True

    def _get_conn(self) -> sqlite3.Connection:
        """获取SQLite连接。"""
        conn = sqlite3.connect(str(self._db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _ensure_connected(self) -> None:
        """确认已连接QMT，否则抛出异常。"""
        if not self._connected:
            raise ConnectionError("QMT未连接，请先调用 connect()")

    def _validate_stock_code(self, code: str) -> None:
        """验证股票代码是否在允许范围内。

        只允许沪深A股: 00xxxx(深主板) 30xxxx(创业板) 60xxxx(沪主板)
        排除: 科创板(68x) 北交所(8x/9x) 基金/债券/ETF等。

        Args:
            code: 6位证券代码

        Raises:
            TradeSafetyError: 代码不在允许范围内
        """
        if not code or len(code) != 6 or not code.isdigit():
            raise TradeSafetyError(f"无效的证券代码: '{code}'，应为6位数字")

        allowed = any(code.startswith(prefix) for prefix in self._allowed_prefixes)
        if not allowed:
            raise TradeSafetyError(
                f"证券代码 {code} 不在允许范围内，"
                f"允许的前缀: {self._allowed_prefixes}（沪深主板+创业板）"
            )

    def _check_daily_limit(self) -> None:
        """检查今日交易次数是否已达上限。

        Raises:
            TradeSafetyError: 今日交易次数已达上限
        """
        today = date.today().isoformat()
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM trader_orders WHERE DATE(created_at) = ?",
                (today,),
            ).fetchone()
            count = row["cnt"] if row else 0
        finally:
            conn.close()

        if count >= self._max_daily_trades:
            raise TradeSafetyError(
                f"今日已下单 {count} 次，达到上限 {self._max_daily_trades} 次"
            )

    def _check_single_amount(self, price: float, amount: int) -> None:
        """检查单笔交易金额是否超限。

        Args:
            price: 委托价格
            amount: 委托数量

        Raises:
            TradeSafetyError: 单笔金额超限
        """
        total = price * amount
        if total > self._max_single_amount:
            raise TradeSafetyError(
                f"单笔交易金额 {total:,.0f} 元超过上限 {self._max_single_amount:,.0f} 元"
            )

    def _check_amount_lot(self, amount: int, direction: str) -> None:
        """检查委托数量是否为100的整数倍(A股必须整手交易)。

        卖出允许零股(因分红送股等产生的不足100股)。

        Args:
            amount: 委托数量
            direction: "buy" / "sell"

        Raises:
            TradeSafetyError: 买入数量非整手
        """
        if direction == "buy" and amount % 100 != 0:
            raise TradeSafetyError(f"买入数量必须为100的整数倍，当前: {amount}")
        if amount <= 0:
            raise TradeSafetyError(f"委托数量必须大于0，当前: {amount}")

    def _record_order(
        self,
        order_id: int,
        stock_code: str,
        direction: str,
        price: float,
        amount: int,
        strategy: str = "",
    ) -> None:
        """将订单记录写入SQLite。

        Args:
            order_id: 系统订单号
            stock_code: 证券代码
            direction: "buy" / "sell"
            price: 委托价格
            amount: 委托数量
            strategy: 策略标签
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = self._get_conn()
        try:
            conn.execute(
                """INSERT OR REPLACE INTO trader_orders
                   (order_id, stock_code, direction, price, amount,
                    status, order_time, strategy, created_at, updated_at)
                   VALUES (?, ?, ?, ?, ?, 'submitted', ?, ?, ?, ?)""",
                (str(order_id), stock_code, direction, price, amount,
                 now_str, strategy, now_str, now_str),
            )
            conn.commit()
        finally:
            conn.close()

        logger.info(
            "订单已记录: id=%s code=%s %s %.2f×%d",
            order_id, stock_code, direction, price, amount,
        )

    def _update_order_status(
        self,
        order_id: str,
        status: str,
        filled_price: float = 0.0,
        filled_amount: int = 0,
        message: str = "",
    ) -> None:
        """更新订单状态。

        Args:
            order_id: 订单号
            status: 新状态
            filled_price: 成交均价
            filled_amount: 成交数量
            message: 状态消息
        """
        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn = self._get_conn()
        try:
            conn.execute(
                """UPDATE trader_orders
                   SET status=?, filled_price=?, filled_amount=?,
                       message=?, updated_at=?
                   WHERE order_id=?""",
                (status, filled_price, filled_amount, message, now_str, str(order_id)),
            )
            conn.commit()
        finally:
            conn.close()

    # -------------------------------------------------------------------
    # 公开接口: 连接管理
    # -------------------------------------------------------------------

    def connect(self) -> bool:
        """连接QMT客户端。

        实盘模式: 通过xtquant连接本地QMT客户端
        Mock模式: 使用模拟数据

        Returns:
            True=连接成功 False=连接失败
        """
        if self._connected:
            logger.warning("QMT已连接，无需重复连接")
            return True

        qmt_path = self._config.get("qmt_path", "")
        account_id = self._config.get("account_id", "")

        if self._mock_mode:
            self._xt = _MockXtQuant()
            self._xt.create_session(qmt_path)
            self._connected = True
            return True

        # ---- 实盘模式 ----
        try:
            from xtquant.xttrader import XtQuantTrader, XtQuantTraderCallback
            from xtquant.xttype import StockAccount
        except ImportError as e:
            logger.error("无法导入xtquant: %s，请确认在Windows环境且QMT已安装", e)
            return False

        if not qmt_path:
            logger.error("QMT路径未配置，请在 config/trader.json 中设置 qmt_path")
            return False

        if not account_id:
            logger.error("资金账号未配置，请在 config/trader.json 中设置 account_id")
            return False

        try:
            self._session_id = int(time.time() * 1000) % 1000000
            self._xt = XtQuantTrader(self._session_id, qmt_path)

            # 注册回调
            callback = _XtCallback(self)
            self._xt.register_callback(callback)

            # 启动
            self._xt.start()

            # 建立连接
            connect_result = self._xt.connect()
            if connect_result != 0:
                logger.error("QMT连接失败，返回码: %s", connect_result)
                return False

            # 订阅账户
            self._account = StockAccount(account_id)
            self._xt.subscribe_account(self._account)

            self._connected = True
            logger.info("QMT连接成功: account=%s path=%s", account_id, qmt_path)
            return True

        except Exception as e:
            logger.error("QMT连接异常: %s", e, exc_info=True)
            self._connected = False
            return False

    def disconnect(self) -> None:
        """断开QMT连接。"""
        if not self._connected:
            return

        try:
            if self._mock_mode:
                self._xt.disconnect()
            else:
                if self._xt is not None:
                    self._xt.stop()
        except Exception as e:
            logger.warning("断开连接时异常: %s", e)
        finally:
            self._connected = False
            self._xt = None
            logger.info("QMT连接已断开")

    @property
    def is_connected(self) -> bool:
        """当前是否已连接。"""
        return self._connected

    @property
    def is_mock(self) -> bool:
        """是否为模拟模式。"""
        return self._mock_mode

    # -------------------------------------------------------------------
    # 公开接口: 账户查询
    # -------------------------------------------------------------------

    def get_positions(self, stock_code: str = "") -> list[Position]:
        """获取当前持仓。

        Args:
            stock_code: 可选，只查询指定代码的持仓。为空则返回全部持仓。

        Returns:
            持仓列表
        """
        self._ensure_connected()
        account_id = self._config.get("account_id", "")

        try:
            if self._mock_mode:
                raw = self._xt.get_stock_position(account_id, stock_code)
                if raw is None:
                    return []
                return [
                    Position(
                        stock_code=p["stock_code"],
                        stock_name=p.get("stock_name", ""),
                        volume=p["volume"],
                        available=p.get("available", p["volume"]),
                        cost_price=p["cost_price"],
                        market_value=p["cost_price"] * p["volume"],
                        profit_loss=0.0,
                    )
                    for p in raw
                ]

            # 实盘模式
            positions = self._xt.query_stock_positions(self._account)
            result = []
            for p in positions:
                if stock_code and p.stock_code != stock_code:
                    continue
                result.append(Position(
                    stock_code=p.stock_code,
                    stock_name=getattr(p, "stock_name", ""),
                    volume=p.volume,
                    available=p.can_use_volume,
                    cost_price=p.avg_price,
                    market_value=getattr(p, "market_value", 0.0),
                    profit_loss=getattr(p, "profit_loss", 0.0),
                ))
            return result

        except Exception as e:
            logger.error("获取持仓失败: %s", e, exc_info=True)
            return []

    def get_balance(self) -> Optional[Balance]:
        """获取账户资金信息。

        Returns:
            Balance对象，查询失败返回None
        """
        self._ensure_connected()
        account_id = self._config.get("account_id", "")

        try:
            if self._mock_mode:
                raw = self._xt.get_asset(account_id)
                if raw is None:
                    return None
                return Balance(
                    total_asset=raw["total_asset"],
                    cash=raw["cash"],
                    market_value=raw["market_value"],
                    frozen_cash=raw.get("frozen_cash", 0.0),
                )

            # 实盘模式
            asset = self._xt.query_asset(self._account)
            if asset is None:
                return None
            return Balance(
                total_asset=asset.total_asset,
                cash=asset.cash,
                market_value=asset.market_value,
                frozen_cash=getattr(asset, "frozen_cash", 0.0),
            )

        except Exception as e:
            logger.error("获取资金信息失败: %s", e, exc_info=True)
            return None

    def get_orders(self) -> list[OrderRecord]:
        """获取当日委托列表。

        Returns:
            当日所有委托记录列表
        """
        self._ensure_connected()
        account_id = self._config.get("account_id", "")

        try:
            if self._mock_mode:
                raw_orders = self._xt.get_orders(account_id)
                result = []
                for oid, o in raw_orders.items():
                    direction = "buy" if o["direction"] == self.DIR_BUY else "sell"
                    status_enum = XT_ORDER_STATUS_MAP.get(o["status"], OrderStatus.UNKNOWN)
                    result.append(OrderRecord(
                        order_id=str(o["order_id"]),
                        stock_code=o["stock_code"],
                        direction=direction,
                        price=o["price"],
                        amount=o["amount"],
                        filled_price=o.get("filled_price", 0.0),
                        filled_amount=o.get("filled_amount", 0),
                        status=status_enum.value,
                        order_time=o.get("order_time", ""),
                    ))
                return result

            # 实盘模式
            orders = self._xt.query_stock_orders(self._account)
            result = []
            for o in orders:
                direction = "buy" if o.order_type == self.DIR_BUY else "sell"
                status_enum = XT_ORDER_STATUS_MAP.get(o.order_status, OrderStatus.UNKNOWN)
                result.append(OrderRecord(
                    order_id=str(o.order_id),
                    stock_code=o.stock_code,
                    direction=direction,
                    price=o.price,
                    amount=o.order_volume,
                    filled_price=getattr(o, "traded_price", 0.0),
                    filled_amount=getattr(o, "traded_volume", 0),
                    status=status_enum.value,
                    order_time=str(getattr(o, "order_time", "")),
                ))
            return result

        except Exception as e:
            logger.error("获取委托列表失败: %s", e, exc_info=True)
            return []

    # -------------------------------------------------------------------
    # 公开接口: 交易操作
    # -------------------------------------------------------------------

    def buy(
        self,
        code: str,
        price: float,
        amount: int,
        strategy: str = "",
    ) -> Optional[str]:
        """买入股票。

        Args:
            code: 证券代码，如 "000001"
            price: 委托价格
            amount: 委托数量（必须为100的整数倍）
            strategy: 策略标签，用于标记订单来源

        Returns:
            成功返回订单号(str)，失败返回None

        Raises:
            ConnectionError: QMT未连接
            TradeSafetyError: 安全检查未通过
        """
        return self._place_order(code, price, amount, "buy", strategy)

    def sell(
        self,
        code: str,
        price: float,
        amount: int,
        strategy: str = "",
    ) -> Optional[str]:
        """卖出股票。

        Args:
            code: 证券代码，如 "000001"
            price: 委托价格
            amount: 委托数量（允许零股卖出）
            strategy: 策略标签

        Returns:
            成功返回订单号(str)，失败返回None

        Raises:
            ConnectionError: QMT未连接
            TradeSafetyError: 安全检查未通过
        """
        return self._place_order(code, price, amount, "sell", strategy)

    def _place_order(
        self,
        code: str,
        price: float,
        amount: int,
        direction: str,
        strategy: str = "",
    ) -> Optional[str]:
        """统一下单入口。

        执行安全检查后提交订单并记录到数据库。

        Args:
            code: 证券代码
            price: 委托价格
            amount: 委托数量
            direction: "buy" / "sell"
            strategy: 策略标签

        Returns:
            成功返回订单号(str)，失败返回None
        """
        # 1. 前置检查
        self._ensure_connected()
        self._validate_stock_code(code)
        self._check_amount_lot(amount, direction)
        self._check_single_amount(price, amount)
        self._check_daily_limit()

        if price <= 0:
            raise TradeSafetyError(f"委托价格必须大于0，当前: {price}")

        # 2. 确定 xtquant 参数
        account_id = self._config.get("account_id", "")
        xt_direction = self.DIR_BUY if direction == "buy" else self.DIR_SELL

        # 3. 提交订单
        try:
            if self._mock_mode:
                order_id = self._xt.order_stock(
                    account_id, code, self.PRICE_FIXED,
                    price, amount, xt_direction, strategy,
                )
            else:
                order_id = self._xt.order_stock(
                    self._account, code, self.PRICE_FIXED,
                    price, amount, xt_direction, strategy,
                )

            if order_id <= 0:
                logger.error("下单失败: 返回order_id=%s code=%s", order_id, code)
                return None

            # 4. 记录到数据库
            self._record_order(order_id, code, direction, price, amount, strategy)

            logger.info(
                "下单成功: id=%s code=%s %s %.2f×%d 金额=%.0f",
                order_id, code, direction, price, amount, price * amount,
            )
            return str(order_id)

        except TradeSafetyError:
            raise
        except Exception as e:
            logger.error("下单异常: code=%s %s %.2f×%d — %s", code, direction, price, amount, e)
            return None

    def cancel_order(self, order_id: str | int) -> bool:
        """撤销委托订单。

        Args:
            order_id: 订单号

        Returns:
            True=撤单成功 False=撤单失败
        """
        self._ensure_connected()
        account_id = self._config.get("account_id", "")

        try:
            if self._mock_mode:
                result = self._xt.cancel_order(account_id, int(order_id))
                success = result == 0
            else:
                result = self._xt.cancel_order_stock(
                    self._account, int(order_id)
                )
                success = result == 0

            if success:
                self._update_order_status(
                    str(order_id), OrderStatus.CANCELLED.value,
                    message="主动撤单",
                )
                logger.info("撤单成功: order_id=%s", order_id)
            else:
                logger.warning("撤单失败: order_id=%s result=%s", order_id, result)

            return success

        except Exception as e:
            logger.error("撤单异常: order_id=%s — %s", order_id, e)
            return False

    # -------------------------------------------------------------------
    # 公开接口: 辅助方法
    # -------------------------------------------------------------------

    def get_today_trade_count(self) -> int:
        """获取今日已下单一数。

        Returns:
            今日下单次数
        """
        today = date.today().isoformat()
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as cnt FROM trader_orders WHERE DATE(created_at) = ?",
                (today,),
            ).fetchone()
            return row["cnt"] if row else 0
        finally:
            conn.close()

    def get_today_orders(self) -> list[dict]:
        """获取今日所有订单记录(从数据库读取)。

        Returns:
            订单字典列表
        """
        today = date.today().isoformat()
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT * FROM trader_orders WHERE DATE(created_at) = ? ORDER BY created_at DESC",
                (today,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_order_by_id(self, order_id: str) -> Optional[dict]:
        """根据订单号查询订单详情。

        Args:
            order_id: 订单号

        Returns:
            订单字典，不存在则返回None
        """
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT * FROM trader_orders WHERE order_id = ?",
                (str(order_id),),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def update_order_from_callback(
        self,
        order_id: int,
        status_code: int,
        filled_price: float = 0.0,
        filled_amount: int = 0,
        message: str = "",
    ) -> None:
        """由回调触发的订单状态更新。

        xtquant回调中调用此方法同步状态到数据库。

        Args:
            order_id: 订单号
            status_code: xtquant状态码
            filled_price: 成交均价
            filled_amount: 成交数量
            message: 状态消息
        """
        status_enum = XT_ORDER_STATUS_MAP.get(status_code, OrderStatus.UNKNOWN)
        self._update_order_status(
            str(order_id),
            status_enum.value,
            filled_price,
            filled_amount,
            message,
        )
        logger.info(
            "订单状态回调更新: id=%s status=%s filled=%.2f×%d",
            order_id, status_enum.value, filled_price, filled_amount,
        )

    def __repr__(self) -> str:
        mode = "MOCK" if self._mock_mode else "LIVE"
        status = "已连接" if self._connected else "未连接"
        return f"<XtGateway [{mode}] {status}>"


# ---------------------------------------------------------------------------
# xtquant回调处理类（仅实盘模式使用）
# ---------------------------------------------------------------------------

class _XtCallback:
    """xtquant交易回调处理器。

    注册到XtQuantTrader，接收订单状态变化推送。
    """

    def __init__(self, gateway: XtGateway) -> None:
        self._gateway = gateway

    def on_order_stock_async_response(
        self, response_type: int, order_id: int, error_id: int, error_msg: str
    ) -> None:
        """异步下单回调。"""
        logger.info(
            "异步下单回调: order_id=%s type=%s error=%s(%s)",
            order_id, response_type, error_msg, error_id,
        )

    def on_stock_order(self, order) -> None:  # type: ignore[override]
        """委托状态变化回调。"""
        try:
            self._gateway.update_order_from_callback(
                order_id=order.order_id,
                status_code=order.order_status,
                filled_price=getattr(order, "traded_price", 0.0),
                filled_amount=getattr(order, "traded_volume", 0),
            )
        except Exception as e:
            logger.error("处理委托回调异常: %s", e, exc_info=True)

    def on_stock_trade(self, trade) -> None:  # type: ignore[override]
        """成交通知回调。"""
        logger.info(
            "成交通知: order_id=%s code=%s price=%.2f volume=%d",
            trade.order_id, trade.stock_code,
            trade.traded_price, trade.traded_volume,
        )

    def on_disconnected(self) -> None:
        """连接断开回调。"""
        logger.warning("QMT连接断开!")
        self._gateway._connected = False

    def on_account_status(self, status) -> None:  # type: ignore[override]
        """账户状态变化回调。"""
        logger.info("账户状态变化: %s", status)

    def on_stock_position(self, position) -> None:  # type: ignore[override]
        """持仓变化回调。"""
        logger.debug("持仓变化: %s", position)

    def on_order_error(
        self, order_id: int, error_id: int, error_msg: str
    ) -> None:
        """下单错误回调。"""
        logger.error("下单错误: order_id=%s error=%s(%s)", order_id, error_msg, error_id)
        try:
            self._gateway.update_order_from_callback(
                order_id=order_id,
                status_code=53,  # REJECTED
                message=f"下单错误: {error_msg}({error_id})",
            )
        except Exception as e:
            logger.error("处理下单错误回调异常: %s", e)

    def on_cancel_error(
        self, order_id: int, error_id: int, error_msg: str
    ) -> None:
        """撤单错误回调。"""
        logger.error("撤单错误: order_id=%s error=%s(%s)", order_id, error_msg, error_id)

    def on_order_stock_error(
        self, response_type: int, order_id: int, error_id: int, error_msg: str
    ) -> None:
        """异步下单错误回调。"""
        logger.error(
            "异步下单错误: order_id=%s type=%s error=%s(%s)",
            order_id, response_type, error_msg, error_id,
        )


# ---------------------------------------------------------------------------
# 便捷函数
# ---------------------------------------------------------------------------

def create_gateway(
    config_path: Path = CONFIG_PATH,
    db_path: Path = DEFAULT_DB_PATH,
) -> XtGateway:
    """创建并返回一个XtGateway实例。

    Args:
        config_path: 配置文件路径
        db_path: 数据库路径

    Returns:
        未连接的XtGateway实例

    使用示例::

        gw = create_gateway()
        if gw.connect():
            balance = gw.get_balance()
            print(balance)
            gw.disconnect()
    """
    return XtGateway(config_path=config_path, db_path=db_path)
