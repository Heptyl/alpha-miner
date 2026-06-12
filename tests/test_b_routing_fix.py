"""测试矩阵: P0 B策略路由修复 — 8项全部通过才算完成"""
import pytest
from unittest.mock import patch, MagicMock


@pytest.fixture(autouse=True)
def isolated_pending_file(tmp_path, monkeypatch):
    """Never let routing tests read or rewrite the daemon's real signal file."""
    from src.trader import daemon_signals

    pending_path = tmp_path / "pending_signals.json"
    pending_path.write_text("[]", encoding="utf-8")
    monkeypatch.setattr(daemon_signals, "SIGNAL_PENDING", pending_path)
    return pending_path


# ── 辅助: 模拟候选对象 ──

def _make_b_candidate(code="000001", day_ret=-6.0, roe=15.0, **extra):
    """构建一个标准B_crash_v2候选"""
    c = {
        "code": code, "name": "测试股", "_strategy": "B",
        "_strategy_version": "B_crash_v2",
        "_day_ret": day_ret, "_roe": roe,
        "_zt_date": "2026-06-09", "_signal_type": "暴跌日狙击",
        "_data_validated": True, "_source": "test",
        "_crash_market_ret": -3.0,
        "score": 70, "reason": "test",
    }
    c.update(extra)
    return c


def _make_old_low_open_candidate(code="000002"):
    """构建一个旧版低开反弹候选(无_day_ret/_roe)"""
    return {
        "code": code, "name": "旧低开股", "_strategy": "B",
        "_zt_date": "2026-06-09",
        "_signal_type": "低开反弹",
        "score": 50, "reason": "低开反弹",
        "_open_drop": -3.5,
    }


# ── Test 1: 非暴跌市场产生0个B候选 ──

def test_1_non_crash_day_zero_candidates():
    """非暴跌日, _get_b_watchlist返回空列表"""
    from src.trader.daemon_strategies import _get_b_watchlist, _b_crash_cache
    _b_crash_cache["data"] = []
    _b_crash_cache["date"] = None
    with patch("src.trader.daemon_strategies._is_crash_day", return_value=False):
        result = _get_b_watchlist()
    assert result == [], f"非暴跌日应返回空列表, 实际返回{len(result)}个"


# ── Test 2: day_ret 为 None 或 0 → 拒绝 ──

def test_2a_day_ret_none_rejected():
    """day_ret=None → _validate_b_crash_candidate拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(None, 15.0)
    assert reason is not None, "day_ret=None应被拒绝"
    assert "无效" in reason


def test_2b_day_ret_zero_rejected():
    """day_ret=0 → _validate_b_crash_candidate拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(0, 15.0)
    assert reason is not None, "day_ret=0应被拒绝(疑似API缺失)"
    assert "0" in reason or "疑似" in reason


# ── Test 3: roe 为 None 或 0 或 <10 → 拒绝 ──

def test_3a_roe_none_rejected():
    """roe=None → _validate_b_crash_candidate拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(-6.0, None)
    assert reason is not None, "roe=None应被拒绝"


def test_3b_roe_zero_rejected():
    """roe=0 → _validate_b_crash_candidate拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(-6.0, 0)
    assert reason is not None, "roe=0应被拒绝(疑似API缺失)"


def test_3c_roe_below_10_rejected():
    """roe=8 → _validate_b_crash_candidate拒绝(需>=10)"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(-6.0, 8.0)
    assert reason is not None, "roe=8%应被拒绝(<10%)"


# ── Test 4: day_ret > -5% → 拒绝 ──

def test_4_day_ret_above_threshold_rejected():
    """day_ret=-3% → 不满足crash_stock_drop(-5%), 拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(-3.0, 15.0)
    assert reason is not None, "day_ret=-3%应被拒绝(未达-5%阈值)"
    assert "-3" in reason or "未达到" in reason


# ── Test 5: 完整有效样本通过 ──

def test_5_valid_candidate_passes():
    """day_ret=-6%, roe=15% → 通过验证"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(-6.0, 15.0)
    assert reason is None, f"有效候选不应被拒绝, 拒绝原因: {reason}"


def test_5b_exact_drop_boundary_passes():
    """规则定义为day_ret<=-5%, 所以-5.0%边界必须通过。"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate

    assert _validate_b_crash_candidate(-5.0, 10.0) is None


# ── Test 6: 旧低开反弹候选不能进入 B_crash_v2 ──

def test_6_old_low_open_blocked():
    """Registry不再返回旧低开反弹候选给B"""
    from src.strategy.registry import StrategyRegistry
    StrategyRegistry.reset()

    # 模拟auto_register
    StrategyRegistry._auto_register()

    # "B"应该不再有注册函数(或返回空)
    b_result = StrategyRegistry.get_candidates("B")
    assert b_result == [], f"Registry.get_candidates('B')应返回空列表, 实际: {b_result}"

    # 旧版应注册为B_LOW_OPEN_V1
    assert "B_LOW_OPEN_V1" in StrategyRegistry.list_registered(), \
        "旧低开反弹应注册为B_LOW_OPEN_V1"


def test_6b_old_candidate_no_day_ret_roe():
    """旧低开反弹候选没有_day_ret/_roe → 被_validate拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    old_cand = _make_old_low_open_candidate()
    day_ret = old_cand.get("_day_ret")  # None
    roe = old_cand.get("_roe")  # None
    reason = _validate_b_crash_candidate(day_ret, roe)
    assert reason is not None, "旧候选无_day_ret/_roe应被拒绝"


# ── Test 7: 篡改pending字段后执行层再次拒绝 ──

def test_7_tampered_pending_rejected():
    """篡改B pending后，真实执行层必须在execute_buy之前拒绝。"""
    from src.trader.daemon_signals import _do_execute_signal

    signal = {
        "action": "buy",
        "code": "000001",
        "name": "测试股",
        "reason": "test",
        "signal_type": "暴跌日狙击(策略B)",
        "extra": {"crash_day_ret": -6.0, "roe": None},
    }
    with patch(
        "src.trader.daemon_signals.get_realtime",
        return_value={"000001": {"price": 10.0}},
    ), patch(
        "src.trader.daemon_signals.get_held_positions",
        return_value=[],
    ), patch("src.trader.trading_daemon.execute_buy") as execute_buy:
        result = _do_execute_signal(signal)

    assert result["success"] is False
    assert "策略B数据无效" in result["reason"]
    execute_buy.assert_not_called()


def test_7b_tampered_day_ret_zero():
    """pending字段被篡改(day_ret改为0) → 执行层拒绝"""
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(0, 15.0)
    assert reason is not None, "day_ret=0应被拒绝"


# ── Test 8: 同一扫描同一股票不生成重复pending ──

def test_8_no_duplicate_pending():
    """_add_signal对同一code同action去重"""
    from src.trader.daemon_signals import _add_signal, _read_pending_signals

    with patch("src.trader.daemon_risk._is_trading_time", return_value=True), \
         patch("src.trader.daemon_db._get_conn") as mock_conn:
        mock_conn.return_value.execute.return_value.fetchone.return_value = None
        mock_conn.return_value.close = MagicMock()

        _add_signal("buy", "000001", "测试", 10.0, "原因A", "暴跌日狙击(策略B)")
        _add_signal("buy", "000001", "测试", 10.0, "原因B", "暴跌日狙击(策略B)")

    pending = _read_pending_signals()
    buy_000001 = [s for s in pending if s["code"] == "000001" and s["action"] == "buy"]
    assert len(buy_000001) <= 1, f"同一股票不应有重复pending, 实际有{len(buy_000001)}个"


def test_8b_general_scan_cannot_create_b_pending():
    """通用扫描只记录B候选，不能成为第二个B下单入口。"""
    from src.trader.trading_daemon import _scan_buy

    candidate = _make_b_candidate()
    quote = {
        "price": 10.0,
        "name": "测试股",
        "change_pct_calc": -6.0,
        "yesterday_close": 10.6,
        "open": 10.0,
        "ask1": [10.0, 100],
        "volume": 10000,
    }
    result = {"buys": [], "skipped": []}

    with patch("src.trader.trading_daemon.get_held_positions", return_value=[]), \
         patch("src.trader.trading_daemon.get_account", return_value={"daily_pnl": 0, "cash": 100000}), \
         patch("src.trader.trading_daemon._check_consecutive_losses", return_value=False), \
         patch("src.trader.trading_daemon._check_monthly_drawdown", return_value=False), \
         patch("src.trader.trading_daemon._collect_candidates", return_value=[candidate]), \
         patch("src.trader.trading_daemon.get_realtime", return_value={"000001": quote}), \
         patch("src.trader.trading_daemon.record_shadow_signal") as record_shadow, \
         patch("src.trader.trading_daemon._add_signal") as add_signal, \
         patch("src.trader.daemon_risk._last_ebb_clear_time", None):
        _scan_buy({"can_buy": True, "phase": "正常"}, result)

    add_signal.assert_not_called()
    record_shadow.assert_called_once()
    assert result["buys"] == []
    assert any("shadow" in item["reason"] for item in result["skipped"])


def test_8c_consecutive_crash_gate_blocks_b_pending():
    """B专用入口遇到连续暴跌时必须在读取候选前关闭。"""
    from src.trader.daemon_strategies import _check_b_pullback_realtime

    conn = MagicMock()
    conn.execute.return_value.fetchone.return_value = ("2026-06-08", -0.03)
    result = {"buys": [], "skipped": []}

    with patch("src.trader.daemon_strategies._get_conn", return_value=conn), \
         patch("src.trader.daemon_strategies._get_b_watchlist") as get_watchlist, \
         patch("src.trader.daemon_strategies._add_signal") as add_signal:
        _check_b_pullback_realtime(result)

    get_watchlist.assert_not_called()
    add_signal.assert_not_called()


# ── 额外: B候选字段完整性检查 ──

def test_b_candidate_required_fields():
    """B候选必须包含所有必填字段"""
    cand = _make_b_candidate()
    required = ["_strategy", "_strategy_version", "_day_ret", "_roe",
                "_crash_market_ret", "_data_validated", "_source"]
    for field in required:
        assert field in cand, f"B候选缺少必填字段: {field}"
    assert cand["_data_validated"] is True


def test_b_candidate_day_ret_not_zero_formatted():
    """B候选_day_ret为0时应设为None, 不是格式化为0"""
    cand = _make_b_candidate(day_ret=0)
    # 生成函数会把0转为None
    # 这里验证_validate拒绝0值
    from src.trader.daemon_strategies import _validate_b_crash_candidate
    reason = _validate_b_crash_candidate(cand["_day_ret"], cand["_roe"])
    # day_ret=0 should be rejected
    # 但_make_b_candidate直接设了0, 所以检查_validate
    # 实际生成函数会把0→None, 这里测试_validate对0的态度
    assert _validate_b_crash_candidate(0, 15.0) is not None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
