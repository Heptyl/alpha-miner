"""日报生成器 — 市场概况 + 有效因子 + 漂移预警 + 挖掘结果 + 明日候选。"""

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd

from src.data.storage import Storage
from src.drift.report import DriftReport
from src.drift.regime import RegimeDetector
from src.drift.ic_tracker import ICTracker
from src.factors.registry import FactorRegistry


class DailyReport:
    """生成每日综合报告。"""

    def __init__(self, db: Storage, mining_log_path: str = "data/mining_log.jsonl"):
        self.db = db
        self.drift_report = DriftReport(db)
        self.regime_detector = RegimeDetector(db)
        self.ic_tracker = ICTracker(db)
        self.registry = FactorRegistry()
        self.mining_log_path = Path(mining_log_path)

    def generate(self, as_of: datetime) -> str:
        """生成完整日报文本。"""
        date_str = as_of.strftime("%Y-%m-%d")
        lines = []

        # ── 标题 ──
        lines.append("=" * 70)
        lines.append(f"  Alpha Miner 日报 — {date_str}")
        lines.append("=" * 70)

        # ── 1. 市场概况 ──
        lines.append(self._section_market(as_of))

        # ── 2. 有效因子排名 ──
        lines.append(self._section_factors(as_of))

        # ── 3. 漂移预警 ──
        lines.append(self._section_drift(as_of))

        # ── 4. 今日挖掘结果 ──
        lines.append(self._section_mining(as_of))

        # ── 5. 明日候选标的 ──
        lines.append(self._section_candidates(as_of))

        # ── 6. 系统状态 ──
        lines.append(self._section_system(as_of))

        lines.append("\n" + "=" * 70)
        return "\n".join(lines)

    # ================================================================
    # 各板块
    # ================================================================

    def _section_market(self, as_of: datetime) -> str:
        """市场概况。"""
        date_str = as_of.strftime("%Y-%m-%d")
        lines = ["\n[1. 市场概况]"]

        # 市场情绪
        market_df = self.db.query("market_emotion", as_of, where="trade_date = ?", params=(date_str,))
        if not market_df.empty:
            row = market_df.iloc[-1]
            zt = int(row.get("zt_count", 0))
            dt = int(row.get("dt_count", 0))
            highest = int(row.get("highest_board", 0))
            lines.append(f"  涨停: {zt}  跌停: {dt}  最高连板: {highest}")
        else:
            zt, dt = 0, 0
            lines.append("  (无市场情绪数据)")

        # 成交额
        price_df = self.db.query("daily_price", as_of, where="trade_date = ?", params=(date_str,))
        if not price_df.empty and "amount" in price_df.columns:
            total_amount = float(price_df["amount"].sum()) / 1e8
            lines.append(f"  全市场成交额: {total_amount:.0f} 亿  个股数: {len(price_df)}")
        else:
            lines.append("  (无行情数据)")

        # Regime
        regime = self.regime_detector.detect(as_of)
        regime_cn = {
            "board_rally": "连板潮",
            "theme_rotation": "题材轮动",
            "low_volume": "地量",
            "broad_move": "普涨/普跌",
            "normal": "正常",
        }
        lines.append(f"  市场状态: {regime_cn.get(regime.regime, regime.regime)} (置信度 {regime.confidence:.0%})")

        return "\n".join(lines)

    def _section_factors(self, as_of: datetime) -> str:
        """有效因子排名。"""
        lines = ["\n[2. 有效因子排名]"]

        factor_names = self.registry.list_factors()
        if not factor_names:
            lines.append("  (无注册因子)")
            return "\n".join(lines)

        factor_stats = []
        for name in factor_names:
            status = self.ic_tracker.current_status(name, window=20)
            factor_stats.append({"name": name, **status})

        # 按 ic_avg 降序
        factor_stats.sort(key=lambda x: x.get("ic_avg", 0) if not np.isnan(x.get("ic_avg", 0)) else -999, reverse=True)

        lines.append(f"  {'因子':<25} {'状态':<8} {'IC均值':>8} {'ICIR':>8} {'胜率':>8} {'趋势':<10}")
        lines.append("  " + "-" * 75)

        for f in factor_stats:
            ic_avg = f"{f['ic_avg']:.4f}" if not np.isnan(f.get('ic_avg', np.nan)) else "N/A"
            icir = f"{f['icir']:.2f}" if not np.isnan(f.get('icir', np.nan)) else "N/A"
            wr = f"{f['win_rate']:.0%}" if not np.isnan(f.get('win_rate', np.nan)) else "N/A"
            status = f.get("status", "unknown")
            trend = f.get("trend", "unknown")

            # 标记
            mark = ""
            if status == "healthy":
                mark = " ★"
            elif status == "dead":
                mark = " ✗"

            lines.append(f"  {f['name']:<25} {status:<8} {ic_avg:>8} {icir:>8} {wr:>8} {trend:<10}{mark}")

        # 统计
        healthy = sum(1 for f in factor_stats if f.get("status") == "healthy")
        dead = sum(1 for f in factor_stats if f.get("status") == "dead")
        lines.append(f"\n  有效: {healthy}  失效: {dead}  总计: {len(factor_stats)}")

        return "\n".join(lines)

    def _section_drift(self, as_of: datetime) -> str:
        """漂移预警。"""
        lines = ["\n[3. 漂移预警]"]

        report = self.drift_report.generate(as_of)
        alerts = report.get("alerts", [])

        if not alerts:
            lines.append("  无漂移告警")
        else:
            # 去重
            seen = set()
            unique_alerts = []
            for a in alerts:
                key = (a["type"], a["factor"])
                if key not in seen:
                    seen.add(key)
                    unique_alerts.append(a)

            for a in unique_alerts[:20]:  # 最多20条
                lines.append(f"  [{a['type']}] {a['factor']}: {a['detail']}")

            if len(unique_alerts) > 20:
                lines.append(f"  ... 及其他 {len(unique_alerts) - 20} 条")

        return "\n".join(lines)

    def _section_mining(self, as_of: datetime) -> str:
        """今日挖掘结果。"""
        lines = ["\n[4. 今日挖掘结果]"]

        if not self.mining_log_path.exists():
            lines.append("  (无挖掘记录)")
            return "\n".join(lines)

        date_str = as_of.strftime("%Y-%m-%d")
        records = []
        for line in self.mining_log_path.read_text().strip().split("\n"):
            try:
                r = json.loads(line)
                if r.get("timestamp", "").startswith(date_str):
                    records.append(r)
            except (json.JSONDecodeError, AttributeError):
                continue

        if not records:
            lines.append("  今日无挖掘活动")
            return "\n".join(lines)

        accepted = [r for r in records if r.get("accepted")]
        rejected = [r for r in records if not r.get("accepted")]

        lines.append(f"  评估: {len(records)} 个  验收: {len(accepted)}  淘汰: {len(rejected)}")

        if accepted:
            lines.append("\n  验收因子:")
            for r in accepted:
                ic = r.get("evaluation", {}).get("ic_mean", 0)
                source = r.get("source", "")
                lines.append(f"    {r['name']:<30} IC={ic:.4f}  来源={source}")

        return "\n".join(lines)

    def _section_candidates(self, as_of: datetime) -> str:
        """明日候选标的 — 有效因子加权打分，regime 调权。"""
        lines = ["\n[5. 明日候选标的]"]

        date_str = as_of.strftime("%Y-%m-%d")
        factor_names = self.registry.list_factors()

        if not factor_names:
            lines.append("  (无注册因子)")
            return "\n".join(lines)

        # 获取 regime
        regime = self.regime_detector.detect(as_of)
        regime_weights = {
            "board_rally": {"consecutive_board": 2.0, "zt_dt_ratio": 1.5, "leader_clarity": 1.8},
            "theme_rotation": {"theme_crowding": 1.8, "narrative_velocity": 1.5, "theme_lifecycle": 1.5},
            "low_volume": {},
            "broad_move": {},
            "normal": {},
        }
        rw = regime_weights.get(regime.regime, {})

        # 收集所有有效因子的截面值
        scores = pd.DataFrame()
        valid_factors = 0

        for name in factor_names:
            status = self.ic_tracker.current_status(name, window=20)
            # 跳过失效因子
            if status.get("status") in ("dead", "negative", "no_data"):
                continue

            # 获取因子值
            fv_df = self.db.query(
                "factor_values",
                as_of,
                where="factor_name = ? AND trade_date = ?",
                params=(name, date_str),
            )
            if fv_df.empty:
                continue

            # 去重
            fv_df = fv_df.sort_values("snapshot_time").groupby("stock_code").last().reset_index()

            ic = status.get("ic_avg", 0)
            if np.isnan(ic):
                ic = 0

            # 权重 = |IC| * regime调权
            weight = abs(ic)
            if name in rw:
                weight *= rw[name]

            if scores.empty:
                scores = fv_df[["stock_code"]].copy()
                scores["score"] = 0.0

            fv = fv_df.set_index("stock_code")["factor_value"]
            common = scores.index.intersection(fv.index) if "stock_code" not in scores.columns else scores["stock_code"]

            if "stock_code" in scores.columns:
                scores = scores.set_index("stock_code")

            for code in scores.index:
                if code in fv.index and pd.notna(fv[code]):
                    scores.loc[code, "score"] += fv[code] * weight

            scores = scores.reset_index()
            valid_factors += 1

        if valid_factors == 0 or scores.empty:
            lines.append("  (无有效因子数据，无法生成候选)")
            return "\n".join(lines)

        # 排名
        if "stock_code" in scores.columns:
            scores = scores.set_index("stock_code")
        scores = scores.sort_values("score", ascending=False)

        top_n = min(10, len(scores))
        lines.append(f"  基于 {valid_factors} 个有效因子 | regime={regime.regime}")
        lines.append(f"\n  {'排名':>4}  {'代码':<10}  {'综合得分':>10}")
        lines.append("  " + "-" * 30)

        for i, (code, row) in enumerate(scores.head(top_n).iterrows()):
            lines.append(f"  {i+1:>4}  {code:<10}  {row['score']:>10.4f}")

        return "\n".join(lines)

    def _section_system(self, as_of: datetime) -> str:
        """系统状态。"""
        lines = ["\n[6. 系统状态]"]

        date_str = as_of.strftime("%Y-%m-%d")

        # 数据量统计
        tables = ["daily_price", "zt_pool", "zb_pool", "lhb_detail", "fund_flow",
                   "news", "concept_mapping", "concept_daily", "factor_values"]
        for table in tables:
            try:
                df = self.db.query(table, as_of, limit=1)
                if not df.empty:
                    count_df = self.db.query(table, as_of)
                    lines.append(f"  {table:<20} {len(count_df)} rows")
                else:
                    lines.append(f"  {table:<20} 0 rows")
            except Exception:
                lines.append(f"  {table:<20} (表不存在)")

        # 挖掘日志
        if self.mining_log_path.exists():
            log_lines = self.mining_log_path.read_text().strip().split("\n")
            lines.append(f"  {'mining_log':<20} {len(log_lines)} records")
        else:
            lines.append(f"  {'mining_log':<20} 0 records")

        return "\n".join(lines)
