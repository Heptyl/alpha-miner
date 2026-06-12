"""测试华泰QMT/xtquant交易网关。"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path
from unittest.mock import patch

import pytest

from src.trader.xt_gateway import (
    XtGateway,
    OrderStatus,
    Position,
    Balance,
    OrderRecord,
    TradeSafetyError,
    ConnectionError,
    create_gateway,
    _init_orders_table,
    load_trader_config,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_config(tmp_path: Path) -> Path:
    """创建临时配置文件。"""
    cfg = {
        "qmt_path": "/tmp/qmt",
        "account_id": "12345678",
        "account_type": "STOCK",
        "max_daily_trades": 5,
        "max_single_amount": 50000,
        "allowed_prefixes": ["00", "30", "60"],
        "mock_mode": True,
    }
    config_file = tmp_path / "trader.json"
    config_file.write_text(json.dumps(cfg), encoding="utf-8")
    return config_file


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    """创建临时数据库路径。"""
    return tmp_path / "test.db"


@pytest.fixture
def gateway(tmp_config: Path, tmp_db: Path) -> XtGateway:
    """创建已连接的mock网关。"""
    gw = XtGateway(config_path=tmp_config, db_path=tmp_db)
    assert gw.connect()
    return gw


# ---------------------------------------------------------------------------
# 基础测试
# ---------------------------------------------------------------------------

class TestXtGatewayInit:
    """网关初始化测试。"""

    def test_mock_mode_auto_enabled(self, tmp_config: Path, tmp_db: Path) -> None:
        """非Windows环境自动启用mock模式。"""
        gw = XtGateway(config_path=tmp_config, db_path=tmp_db)
        assert gw.is_mock is True

    def test_repr(self, gateway: XtGateway) -> None:
        assert "MOCK" in repr(gateway)
        assert "已连接" in repr(gateway)

    def test_double_connect(self, gateway: XtGateway) -> None:
        """重复连接不会报错。"""
        assert gateway.connect() is True


class TestConnection:
    """连接管理测试。"""

    def test_connect(self, gateway: XtGateway) -> None:
        assert gateway.is_connected is True

    def test_disconnect(self, gateway: XtGateway) -> None:
        gateway.disconnect()
        assert gateway.is_connected is False

    def test_disconnect_when_not_connected(self, tmp_config: Path, tmp_db: Path) -> None:
        """未连接时断开不报错。"""
        gw = XtGateway(config_path=tmp_config, db_path=tmp_db)
        gw.disconnect()
        assert gw.is_connected is False

    def test_ensure_connected_raises(self, tmp_config: Path, tmp_db: Path) -> None:
        """未连接时调用查询方法应抛出异常。"""
        gw = XtGateway(config_path=tmp_config, db_path=tmp_db)
        with pytest.raises(ConnectionError):
            gw.get_balance()


# ---------------------------------------------------------------------------
# 查询测试
# ---------------------------------------------------------------------------

class TestQueries:
    """账户查询测试。"""

    def test_get_balance(self, gateway: XtGateway) -> None:
        balance = gateway.get_balance()
        assert balance is not None
        assert isinstance(balance, Balance)
        assert balance.cash == 50000.0
        assert balance.total_asset == 150000.0

    def test_get_positions_all(self, gateway: XtGateway) -> None:
        positions = gateway.get_positions()
        assert len(positions) == 2
        assert positions[0].stock_code == "000001"
        assert positions[1].stock_code == "600036"

    def test_get_positions_single(self, gateway: XtGateway) -> None:
        positions = gateway.get_positions("000001")
        assert len(positions) == 1
        assert positions[0].stock_code == "000001"
        assert positions[0].volume == 1000

    def test_get_positions_not_found(self, gateway: XtGateway) -> None:
        positions = gateway.get_positions("999999")
        assert len(positions) == 0

    def test_get_orders_empty(self, gateway: XtGateway) -> None:
        orders = gateway.get_orders()
        assert isinstance(orders, list)


# ---------------------------------------------------------------------------
# 交易测试
# ---------------------------------------------------------------------------

class TestTrading:
    """交易操作测试。"""

    def test_buy_success(self, gateway: XtGateway) -> None:
        order_id = gateway.buy("000001", 13.50, 100)
        assert order_id is not None
        assert order_id.isdigit()

    def test_sell_success(self, gateway: XtGateway) -> None:
        order_id = gateway.sell("600036", 36.00, 100)
        assert order_id is not None

    def test_buy_records_in_db(self, gateway: XtGateway) -> None:
        order_id = gateway.buy("000001", 13.50, 100)
        record = gateway.get_order_by_id(order_id)
        assert record is not None
        assert record["stock_code"] == "000001"
        assert record["direction"] == "buy"
        assert record["price"] == 13.50
        assert record["amount"] == 100

    def test_get_today_orders(self, gateway: XtGateway) -> None:
        gateway.buy("000001", 13.50, 100)
        gateway.buy("600036", 36.00, 200)
        orders = gateway.get_today_orders()
        assert len(orders) == 2

    def test_get_today_trade_count(self, gateway: XtGateway) -> None:
        assert gateway.get_today_trade_count() == 0
        gateway.buy("000001", 13.50, 100)
        assert gateway.get_today_trade_count() == 1


class TestCancelOrder:
    """撤单测试。"""

    def test_cancel_success(self, gateway: XtGateway) -> None:
        order_id = gateway.buy("000001", 13.50, 100)
        result = gateway.cancel_order(order_id)
        assert result is True
        record = gateway.get_order_by_id(order_id)
        assert record["status"] == "cancelled"


# ---------------------------------------------------------------------------
# 安全机制测试
# ---------------------------------------------------------------------------

class TestSafetyChecks:
    """安全检查测试。"""

    def test_invalid_code_empty(self, gateway: XtGateway) -> None:
        with pytest.raises(TradeSafetyError, match="无效的证券代码"):
            gateway.buy("", 10.0, 100)

    def test_invalid_code_short(self, gateway: XtGateway) -> None:
        with pytest.raises(TradeSafetyError, match="无效的证券代码"):
            gateway.buy("00001", 10.0, 100)

    def test_invalid_code_non_digit(self, gateway: XtGateway) -> None:
        with pytest.raises(TradeSafetyError, match="无效的证券代码"):
            gateway.buy("00000A", 10.0, 100)

    def test_disallowed_prefix_kcb(self, gateway: XtGateway) -> None:
        """科创板(688)应被拒绝。"""
        with pytest.raises(TradeSafetyError, match="不在允许范围内"):
            gateway.buy("688001", 50.0, 100)

    def test_disallowed_prefix_bse(self, gateway: XtGateway) -> None:
        """北交所(8开头)应被拒绝。"""
        with pytest.raises(TradeSafetyError, match="不在允许范围内"):
            gateway.buy("830001", 10.0, 100)

    def test_buy_not_lot_size(self, gateway: XtGateway) -> None:
        """买入数量非100整数倍应被拒绝。"""
        with pytest.raises(TradeSafetyError, match="100的整数倍"):
            gateway.buy("000001", 10.0, 150)

    def test_sell_allows_odd_lot(self, gateway: XtGateway) -> None:
        """卖出允许零股。"""
        order_id = gateway.sell("000001", 10.0, 50)
        assert order_id is not None

    def test_zero_amount(self, gateway: XtGateway) -> None:
        with pytest.raises(TradeSafetyError, match="必须大于0"):
            gateway.buy("000001", 10.0, 0)

    def test_negative_price(self, gateway: XtGateway) -> None:
        with pytest.raises(TradeSafetyError, match="必须大于0"):
            gateway.buy("000001", -1.0, 100)

    def test_single_amount_exceeds_limit(self, gateway: XtGateway) -> None:
        """单笔金额超限应被拒绝。"""
        with pytest.raises(TradeSafetyError, match="单笔交易金额"):
            gateway.buy("000001", 600.0, 100)  # 60000 > 50000

    def test_daily_trade_limit(self, gateway: XtGateway) -> None:
        """每日交易次数限制（配置为5次）。"""
        for i in range(5):
            gateway.buy("000001", 10.0, 100)

        with pytest.raises(TradeSafetyError, match="达到上限"):
            gateway.buy("600036", 10.0, 100)


# ---------------------------------------------------------------------------
# 数据库测试
# ---------------------------------------------------------------------------

class TestDatabase:
    """数据库相关测试。"""

    def test_table_auto_created(self, tmp_db: Path) -> None:
        """数据库表自动创建。"""
        _init_orders_table(tmp_db)
        import sqlite3
        conn = sqlite3.connect(str(tmp_db))
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        conn.close()
        assert "trader_orders" in tables

    def test_order_status_update(self, gateway: XtGateway) -> None:
        order_id = gateway.buy("000001", 13.50, 100)
        gateway.update_order_from_callback(int(order_id), 51, 13.60, 100)
        record = gateway.get_order_by_id(order_id)
        assert record["status"] == "filled"
        assert record["filled_price"] == 13.60


# ---------------------------------------------------------------------------
# 配置测试
# ---------------------------------------------------------------------------

class TestConfig:
    """配置加载测试。"""

    def test_load_missing_config(self, tmp_path: Path) -> None:
        cfg = load_trader_config(tmp_path / "nonexistent.json")
        assert cfg["mock_mode"] is True
        assert cfg["max_daily_trades"] == 20

    def test_load_valid_config(self, tmp_config: Path) -> None:
        cfg = load_trader_config(tmp_config)
        assert cfg["account_id"] == "12345678"
        assert cfg["max_daily_trades"] == 5


# ---------------------------------------------------------------------------
# 便捷函数测试
# ---------------------------------------------------------------------------

class TestCreateGateway:
    """便捷创建函数测试。"""

    def test_create_gateway(self, tmp_config: Path, tmp_db: Path) -> None:
        gw = create_gateway(config_path=tmp_config, db_path=tmp_db)
        assert isinstance(gw, XtGateway)
        assert not gw.is_connected
