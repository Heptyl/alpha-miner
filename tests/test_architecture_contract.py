"""Protect the few product rules that keep Alpha Miner simple and honest."""

from pathlib import Path

import yaml

ARCHITECTURE = Path(__file__).resolve().parents[1] / "ARCHITECTURE.md"
THEORIES = ARCHITECTURE.parent / "knowledge_base" / "theories.yaml"


def _text() -> str:
    return ARCHITECTURE.read_text(encoding="utf-8")


def test_architecture_stays_small_with_one_engine_user_contract_entry_and_card():
    text = _text()
    assert len(text.splitlines()) <= 120
    for rule in ("一个引擎", "一个 USER 数据契约", "一个 USER 入口", "一种玩法卡"):
        assert rule in text
    assert "<=5秒" in text
    assert "不得新增独立 CLI 或第四套引擎" in text


def test_user_buy_points_are_forward_and_before_limit_up():
    text = _text()
    for rule in (
        "注意力再加速的竞价买点",
        "记忆股或扩散股的趋势/回调/冲板前买点",
        "拥挤衰减/反向瀑布的卖出回避",
        "不要求买入日涨停",
    ):
        assert rule in text


def test_intraday_data_gate_cannot_be_replaced_by_daily_outcome():
    text = _text()
    for required in (
        "成对的 9:25/09:31 带时间戳快照",
        "非陈旧源时钟",
        "单调累计量额",
        "`DATA_NOT_READY/INVALID/UNFILLED`",
    ):
        assert required in text
    assert "盘中封板/回封只用于研究触发质量和成交审计" in text
    assert "不得用盘后日线、`open_count` 或最终封板结果" in text
    assert "不得用盘后日线、`open_count` 或最终封板结果冒充前向盘中证据" in text


def test_windows_collects_server_backtests_and_user_never_waits():
    text = _text()
    assert "Windows 负责免费数据的前向采集与发布" in text
    assert "服务器负责离线回测和慢实验" in text
    assert "慢任务永远不阻塞 USER" in text


def test_paper_and_locked_test_discipline_remain_separate_from_admission():
    text = _text()
    assert "PAPER 与实盘准入" in text
    assert "不能退化为 `WATCH_ONLY`" in text
    assert "未准入仓位仍为 0" in text
    assert "锁定测试只评估一次" in text
    assert "失败候选及后代不得复用" in text
    assert "按**信号日**处理" in text
    for rule in ("成本", "滑点", "不可成交"):
        assert rule in text


def test_retro_filtering_and_forward_paper_are_distinct_evidence_tracks():
    text = _text()
    assert "`RETRO_DEVELOPMENT` 只用于历史筛选" in text
    assert "`FORWARD PAPER` 必须在真实 `decision_time` 冻结候选、触发、放弃、入场、退出与成本" in text
    assert "锁死计划 hash，结算只能单向追加生命周期事件" in text
    assert "成功、失败、无效和未成交都保留并只反馈 development" in text
    assert "holdout 永不反馈演化" in text


def test_behavior_finance_and_limit_up_ecology_are_the_single_product_chain():
    text = _text()
    assert (
        "行为金融理论 → 可证伪假说 → 涨停生态观测\n"
        "  → Behavior State（行为状态）→ 可执行玩法\n"
        "  → 走步/PAPER 证据 → development 反馈演化"
    ) in text
    assert "唯一的涨停板生态研究场景" in text
    assert "涨停是极强的注意力/显著性观测，不是顶层架构或今日候选硬门槛" in text
    assert "holdout 是冻结血统的一次性时间外裁决，不反馈演化" in text


def test_behavior_state_is_one_logical_layer_and_not_an_entry_signal():
    text = _text()
    assert "唯一新增逻辑层，不是新引擎、CLI、数据库或状态文件" in text
    for state in ("attention memory", "diffusion", "crowding", "decay"):
        assert f"`{state}`" in text
    assert "近期涨停记忆池不局限于今日或 D-1" in text
    assert "重复涨停、连板、封板质量和题材扩散强化记忆" in text
    assert "炸板、退潮和破位加速衰减" in text
    assert "冻结实验参数验证，不能写成永恒真理" in text
    assert "行为状态不等于买入信号" in text
    assert "高 attention 不能自动买入" in text


def test_observations_and_candidates_stay_inside_limit_up_ecology():
    text = _text()
    assert "量价、竞价、分钟、题材和新闻" in text
    assert "涨停生态的形成、强化、扩散与衰减" in text
    for universe in ("近期涨停股", "涨停后趋势仍存股", "尚未涨停同题材股"):
        assert universe in text
    assert "主要买点不要求当日涨停" in text


def test_llm_program_boundary_and_architecture_freeze_point_are_explicit():
    text = _text()
    assert "大模型只从行为金融知识" in text
    assert "程序负责行为状态、行情、参数搜索、走步回测" in text
    assert "holdout 结果不得进入演化" in text
    assert "行为状态、走步回放、进化强化三个纵切完成后停止架构扩张" in text
    assert "不再增加逻辑层、引擎、CLI、数据库或平行产品名" in text


def test_theory_knowledge_has_unique_ids_sources_grades_and_testable_targets():
    payload = yaml.safe_load(THEORIES.read_text(encoding="utf-8"))
    grades = set(payload["evidence_grades"])
    assert grades == {"ACADEMIC_FOUNDATION", "THEORY_DERIVED", "HEURISTIC"}
    theories = payload["theories"]
    theory_ids = [item["id"] for item in theories]
    assert len(theory_ids) == len(set(theory_ids))
    assert {
        "有限注意与显著性",
        "信息瀑布与羊群",
        "反应不足与事件后漂移",
        "过度反应、拥挤与反转",
        "参考点与锚定",
    }.issubset({item["name"] for item in theories})
    assert {
        "limited_attention_salience",
        "info_cascade",
        "underreaction_post_event_drift",
        "overreaction_crowding_reversal",
        "reference_point_anchoring",
        "three_shift",
    }.issubset(theory_ids)

    prediction_ids = []
    behavior_states = set()
    for theory in theories:
        assert theory["evidence_grade"] in grades
        assert theory["source"] and theory["testable_target"]
        assert theory["testable_predictions"]
        behavior_states.update(theory["behavior_states"])
        for prediction in theory["testable_predictions"]:
            prediction_ids.append(prediction["id"])
            assert prediction["evidence_grade"] in grades
            assert prediction["source"] and prediction["testable_target"]
            assert prediction["target"] == prediction["testable_target"]
            assert prediction["prediction"] and prediction["factor_type"]
            required_rule = (
                "expression" if prediction["factor_type"] == "formula" else "conditions"
            )
            assert prediction[required_rule]
    assert len(prediction_ids) == len(set(prediction_ids))
    assert behavior_states == {"attention_memory", "diffusion", "crowding", "decay"}

    heuristic = next(item for item in theories if item["id"] == "three_shift")
    assert heuristic["evidence_grade"] == "HEURISTIC"
    assert "不是已确立的学术理论" in heuristic["source"]
    assert {item["evidence_grade"] for item in heuristic["testable_predictions"]} == {
        "HEURISTIC"
    }
