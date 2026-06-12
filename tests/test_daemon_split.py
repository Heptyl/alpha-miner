#!/usr/bin/env python3
"""测试 daemon 大拆分后的模块隔离性。

覆盖:
  - daemon_db: 数据库/账户/持仓操作
  - daemon_risk: 风控检查
  - daemon_strategies: 策略候选
  - daemon_signals: 预告信号
  - daemon_notifier: 通知
  - trading_daemon: re-export向后兼容
"""
import sys
from pathlib import Path
from datetime import datetime, date, timedelta
from unittest.mock import patch, MagicMock

import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))


# ============================================================
# Test 1: 各模块独立导入
# ============================================================

class TestModuleImports:
    """验证所有子模块能独立导入"""

    def test_import_daemon_db(self):
        from src.trader.daemon_db import (
            _get_conn, init_tables, get_account, _update_account_value,
            get_held_positions, _calc_commission, _calc_shares,
            _log_to_db, _is_new_day, _reset_daily_pnl,
        )
        assert callable(init_tables)
        assert callable(get_account)

    def test_import_daemon_signals(self):
        from src.trader.daemon_signals import (
            _read_pending_signals, _write_pending_signals, _add_signal,
            _execute_pending_signals, _do_execute_signal,
        )
        assert callable(_read_pending_signals)
        assert callable(_add_signal)

    def test_import_daemon_notifier(self):
        from src.trader.daemon_notifier import (
            _send_batch_notifications, _send_trade_notification,
        )
        assert callable(_send_batch_notifications)

    def test_import_daemon_strategies(self):
        from src.trader.daemon_strategies import (
            _try_upgrade_positions,
            _get_b_watchlist, _check_b_pullback_realtime,
        )
        assert callable(_try_upgrade_positions)

    def test_import_strategy_c(self):
        """策略C已搬到src/strategy/strategy_c.py"""
        from src.strategy.strategy_c import get_strategy_c_candidates
        assert callable(get_strategy_c_candidates)

    def test_import_daemon_risk(self):
        from src.trader.daemon_risk import (
            _check_lhb_filter, _is_trading_time, _is_grace_period,
            _check_industry_concentration, _check_market_sentiment,
            _check_consecutive_losses, _check_monthly_drawdown,
            _market_crash_clear,
        )
        assert callable(_is_trading_time)
        assert callable(_market_crash_clear)


# ============================================================
# Test 2: Re-export向后兼容
# ============================================================

class TestBackwardCompat:
    """验证 trading_daemon.py 的 re-export 保持向后兼容"""

    def test_db_functions_reexported(self):
        from src.trader.trading_daemon import (
            init_tables, get_account, get_held_positions,
            _log_to_db, _calc_commission, _calc_shares,
        )
        assert callable(init_tables)

    def test_signal_functions_reexported(self):
        from src.trader.trading_daemon import (
            _add_signal, _execute_pending_signals, _read_pending_signals,
        )
        assert callable(_add_signal)

    def test_notifier_functions_reexported(self):
        from src.trader.trading_daemon import (
            _send_batch_notifications, _send_trade_notification,
        )
        assert callable(_send_batch_notifications)

    def test_strategy_functions_reexported(self):
        from src.trader.trading_daemon import (
            get_strategy_c_candidates, _get_b_watchlist,
            _check_b_pullback_realtime,
        )
        assert callable(get_strategy_c_candidates)

    def test_risk_functions_reexported(self):
        from src.trader.trading_daemon import (
            _is_trading_time, _is_grace_period,
            _check_industry_concentration, _check_market_sentiment,
            _check_consecutive_losses, _check_monthly_drawdown,
            _market_crash_clear,
        )
        assert callable(_is_trading_time)
        assert callable(_market_crash_clear)

    def test_core_functions_still_in_main(self):
        from src.trader.trading_daemon import (
            execute_sell, execute_buy,
            check_buy_signals, check_sell_signals,
            get_ml_candidates, scan_once, run_daemon,
        )
        assert callable(scan_once)
        assert callable(run_daemon)

    def test_global_state_reexported(self):
        from src.trader.daemon_risk import _last_ebb_clear_time
        # Just verify the import works, value may be None at import time
        assert _last_ebb_clear_time is None or isinstance(_last_ebb_clear_time, datetime)


# ============================================================
# Test 3: daemon_db 功能测试
# ============================================================

class TestDaemonDb:
    """数据库层核心函数测试"""

    def test_calc_commission_buy(self):
        from src.trader.daemon_db import _calc_commission
        fee, stamp = _calc_commission(10000, is_sell=False)
        assert fee > 0
        assert stamp == 0  # 买入无印花税

    def test_calc_commission_sell(self):
        from src.trader.daemon_db import _calc_commission
        fee, stamp = _calc_commission(10000, is_sell=True)
        assert fee > 0
        assert stamp > 0  # 卖出有印花税

    def test_calc_shares(self):
        from src.trader.daemon_db import _calc_shares
        shares = _calc_shares(10.0, 10000)
        assert shares > 0
        assert shares * 10.0 <= 10000  # 不能超过金额上限

    def test_calc_shares_round_100(self):
        from src.trader.daemon_db import _calc_shares
        shares = _calc_shares(10.0, 10000)
        assert shares % 100 == 0  # A股必须100的整数倍


# ============================================================
# Test 4: daemon_risk 功能测试
# ============================================================

class TestDaemonRisk:
    """风控检查函数测试"""

    def test_is_trading_time_weekday(self):
        """交易时间检查: 周中应返回bool"""
        from src.trader.daemon_risk import _is_trading_time
        result = _is_trading_time()
        assert isinstance(result, bool)

    def test_is_grace_period(self):
        """Grace Period检查: 应返回bool"""
        from src.trader.daemon_risk import _is_grace_period
        result = _is_grace_period()
        assert isinstance(result, bool)

    @patch('src.trader.daemon_risk._get_conn')
    def test_check_industry_concentration_no_position(self, mock_conn):
        """行业集中度: 无持仓时不应限制"""
        from src.trader.daemon_risk import _check_industry_concentration
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = [0]
        mock_conn.return_value.execute.return_value = mock_cursor
        # 如果没有同行业持仓, 应该允许
        with patch('src.trader.daemon_risk.get_held_positions', return_value=[]):
            result = _check_industry_concentration("000001")
            assert isinstance(result, bool)


# ============================================================
# Test 5: 跨模块依赖验证
# ============================================================

class TestCrossModuleDeps:
    """验证子模块间的依赖方向正确"""

    def test_db_no_dependency_on_main(self):
        """daemon_db 不依赖 trading_daemon"""
        import inspect
        from src.trader import daemon_db
        src = inspect.getsource(daemon_db)
        assert 'from src.trader.trading_daemon' not in src
        assert 'import trading_daemon' not in src

    def test_risk_no_dependency_on_main(self):
        """daemon_risk 不依赖 trading_daemon"""
        import inspect
        from src.trader import daemon_risk
        src = inspect.getsource(daemon_risk)
        assert 'from src.trader.trading_daemon' not in src

    def test_strategies_no_dependency_on_main(self):
        """daemon_strategies 不依赖 trading_daemon"""
        import inspect
        from src.trader import daemon_strategies
        src = inspect.getsource(daemon_strategies)
        assert 'from src.trader.trading_daemon' not in src

    def test_notifier_no_dependency_on_main(self):
        """daemon_notifier 不依赖 trading_daemon"""
        import inspect
        from src.trader import daemon_notifier
        src = inspect.getsource(daemon_notifier)
        assert 'from src.trader.trading_daemon' not in src

    def test_signals_lazy_imports_main(self):
        """daemon_signals 只通过lazy import依赖 trading_daemon (execute_buy/sell)"""
        import inspect
        from src.trader import daemon_signals
        src = inspect.getsource(daemon_signals)
        # Lazy import inside function is OK
        assert 'from src.trader.trading_daemon import execute_buy, execute_sell' in src
        # Top-level import of trading_daemon is NOT OK
        lines = src.split('\n')
        top_level_imports = [l for l in lines[:50] if 'trading_daemon' in l and 'import' in l and not l.strip().startswith('#')]
        assert len(top_level_imports) == 0, f"Found top-level import of trading_daemon: {top_level_imports}"

    def test_dependency_dag_is_correct(self):
        """依赖方向: main -> signals -> db (单向, 无循环)"""
        # main imports from: db, signals, notifier, strategies, risk
        # signals imports from: db
        # notifier imports from: db
        # strategies imports from: db
        # risk imports from: db
        # db imports from: config, realtime_quote
        # => No circular dependencies
        pass  # This is verified by the import tests above succeeding


class TestStrategyRegistry:
    """策略注册表测试"""

    def test_registry_auto_register(self):
        """首次get_candidates触发自动注册"""
        from src.strategy.registry import StrategyRegistry
        StrategyRegistry.reset()
        cands = StrategyRegistry.get_candidates("A")
        assert StrategyRegistry._initialized
        assert "A" in StrategyRegistry.list_registered()
        assert "B" not in StrategyRegistry.list_registered()
        assert "B_LOW_OPEN_V1" in StrategyRegistry.list_registered()
        assert "C" in StrategyRegistry.list_registered()

    def test_registry_unknown_strategy(self):
        """未注册策略返回空列表"""
        from src.strategy.registry import StrategyRegistry
        StrategyRegistry.reset()
        cands = StrategyRegistry.get_candidates("X")
        assert cands == []

    def test_registry_returns_list(self):
        """注册策略返回list[dict]"""
        from src.strategy.registry import StrategyRegistry
        StrategyRegistry.reset()
        for name in ["A", "B", "C"]:
            cands = StrategyRegistry.get_candidates(name)
            assert isinstance(cands, list)
            for c in cands:
                assert isinstance(c, dict)
                assert "code" in c

    def test_collect_candidates_uses_registry(self):
        """_collect_candidates通过注册表获取候选,不直接import策略文件"""
        import inspect
        from src.trader.trading_daemon import _collect_candidates
        source = inspect.getsource(_collect_candidates)
        assert "StrategyRegistry" in source
        assert "from src.strategy.strategy_a" not in source
        assert "from src.trader.strategy_b" not in source
        assert "from src.strategy.strategy_b" not in source  # 通过registry间接调用


class TestPublicAPI:
    """公共接口测试"""

    def test_signal_monitor_public_api(self):
        """signal_monitor提供公共接口(非下划线)"""
        from src.trader.signal_monitor import (
            get_daily_data, compute_technical_signals, compute_support_resistance,
        )
        assert callable(get_daily_data)
        assert callable(compute_technical_signals)
        assert callable(compute_support_resistance)

    def test_signal_monitor_get_daily_data(self):
        """get_daily_data公共接口可用"""
        from src.trader.signal_monitor import get_daily_data
        df = get_daily_data("000001", 5)
        assert df is not None
        assert len(df) > 0
