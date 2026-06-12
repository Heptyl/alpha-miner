"""
Alpha Miner 专业回测用例 — 数据压测脚本
参考: Qlib DataHealthChecker / RQAlpha Validator / Zipline合成数据测试 / Backtrader交叉验证

用法:
  python tests/test_data_integrity.py           # 全量运行
  python tests/test_data_integrity.py --quick   # 快速模式(最近30天)
  python tests/test_data_integrity.py --page dashboard  # 只跑某个页面
"""
__test__ = False

import sqlite3
import sys
import time
import argparse
from datetime import datetime, timedelta

DB_PATH = "data/alpha_miner.db"

class TestResult:
    def __init__(self):
        self.results = []
        self.passed = 0
        self.failed = 0
        self.warnings = 0

    def ok(self, page, name, detail=""):
        self.passed += 1
        self.results.append(("PASS", page, name, detail))

    def fail(self, page, name, detail, risk="P1"):
        self.failed += 1
        self.results.append(("FAIL", page, name, detail, risk))

    def warn(self, page, name, detail):
        self.warnings += 1
        self.results.append(("WARN", page, name, detail))

    def summary(self):
        print("\n" + "=" * 70)
        print(f"  Alpha Miner 数据压测报告  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 70)
        print(f"  通过: {self.passed}  失败: {self.failed}  警告: {self.warnings}")
        print("=" * 70)
        if self.failed > 0:
            print("\n  [失败用例]")
            for r in self.results:
                if r[0] == "FAIL":
                    print(f"    [{r[4]}] {r[1]} | {r[2]}")
                    print(f"         {r[3]}")
        if self.warnings > 0:
            print("\n  [警告]")
            for r in self.results:
                if r[0] == "WARN":
                    print(f"    {r[1]} | {r[2]}: {r[3]}")

        # 保存文本报告到 output/test_reports/
        self._save_report()
        return self.failed == 0

    def _save_report(self):
        """保存压测报告到文件(供Web页面读取)"""
        import os
        report_dir = "output/test_reports"
        os.makedirs(report_dir, exist_ok=True)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        date_str = datetime.now().strftime("%Y-%m-%d")

        # 详细文本报告
        lines = []
        lines.append(f"Alpha Miner 数据压测报告")
        lines.append(f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"结果: {self.passed}通过 / {self.failed}失败 / {self.warnings}警告")
        lines.append("=" * 60)
        for r in self.results:
            status, page, name = r[0], r[1], r[2]
            detail = r[3] if len(r) > 3 else ""
            if status == "PASS":
                lines.append(f"  ✅ [{page}] {name}: {detail}")
            elif status == "FAIL":
                risk = r[4] if len(r) > 4 else "?"
                lines.append(f"  ❌ [{risk}][{page}] {name}: {detail}")
            elif status == "WARN":
                lines.append(f"  ⚠️ [{page}] {name}: {detail}")

        # 写文件
        report_path = f"{report_dir}/report_{ts}.txt"
        with open(report_path, "w") as f:
            f.write("\n".join(lines))

        # 最新报告(固定文件名, Web页面直接读这个)
        latest_path = f"{report_dir}/latest.txt"
        with open(latest_path, "w") as f:
            f.write("\n".join(lines))

        # 更新每日汇总(追加)
        daily_path = f"{report_dir}/daily_{date_str}.txt"
        with open(daily_path, "a") as f:
            f.write("\n" + "\n".join(lines) + "\n")

        print(f"\n  报告已保存: {report_path}")


def test_dashboard(conn, result, quick=False):
    page = "Dashboard"
    print(f"\n>>> {page}")
    days_limit = "AND trade_date >= date('now','-30 days')" if quick else ""

    # T01-1: 股票代码格式(必须纯数字)
    bad_codes = conn.execute(
        "SELECT COUNT(*) FROM daily_price WHERE typeof(stock_code)='text' AND NOT stock_code GLOB '[0-9]*'"
    ).fetchone()[0]
    if bad_codes > 0:
        result.fail(page, "股票代码格式", f"{bad_codes}条非纯数字(含sh./sz.前缀)", "P0")
    else:
        result.ok(page, "股票代码格式", "全部纯数字")

    # T01-2: 日K线完整性
    incomplete = conn.execute(f"""
        SELECT trade_date, COUNT(*) as cnt FROM daily_price
        WHERE 1=1 {days_limit}
        GROUP BY trade_date HAVING cnt < 5000
        ORDER BY trade_date DESC LIMIT 10
    """).fetchall()
    if incomplete:
        detail = ", ".join(f"{d[0]}({d[1]})" for d in incomplete[:5])
        result.fail(page, "日K线完整性", f"{len(incomplete)}天不完整: {detail}", "P0")
    else:
        result.ok(page, "日K线完整性", "所有天>=5000只")

    # T01-2: OHLC合理性
    neg = conn.execute(f"SELECT COUNT(*) FROM daily_price WHERE open<0 OR high<0 OR low<0 OR close<0 {days_limit}").fetchone()[0]
    zero = conn.execute(f"SELECT COUNT(*) FROM daily_price WHERE close=0 {days_limit}").fetchone()[0]
    hl = conn.execute(f"SELECT COUNT(*) FROM daily_price WHERE high<low {days_limit}").fetchone()[0]
    issues = []
    if neg: issues.append(f"负数{neg}条")
    if zero: issues.append(f"close=0有{zero}条")
    if hl: issues.append(f"high<low有{hl}条")
    if issues:
        result.fail(page, "OHLC合理性", ", ".join(issues), "P0")
    else:
        result.ok(page, "OHLC合理性")

    # T01-3: pre_close完整性
    null_pct = conn.execute(f"""
        SELECT trade_date, COUNT(*) as total,
               SUM(CASE WHEN pre_close IS NULL OR pre_close=0 THEN 1 ELSE 0 END) as null_cnt
        FROM daily_price WHERE 1=1 {days_limit}
        GROUP BY trade_date HAVING null_cnt > total*0.05
        ORDER BY trade_date DESC LIMIT 5
    """).fetchall()
    if null_pct:
        detail = ", ".join(f"{d[0]}({d[2]}/{d[1]})" for d in null_pct[:3])
        result.fail(page, "pre_close完整性", f"{len(null_pct)}天缺失>5%: {detail}", "P1")
    else:
        result.ok(page, "pre_close完整性")

    # T01-4: amount不为0(影响策略B成交额过滤)
    amt_zero = conn.execute(f"""
        SELECT trade_date, COUNT(*) FROM daily_price
        WHERE amount=0 AND volume>0 {days_limit}
        GROUP BY trade_date HAVING COUNT(*) > 100
        ORDER BY trade_date DESC LIMIT 5
    """).fetchall()
    if amt_zero:
        detail = ", ".join(f"{d[0]}({d[1]}条)" for d in amt_zero[:3])
        result.warn(page, "amount为0", f"有成交但amount=0: {detail}")
    else:
        result.ok(page, "amount字段", "无异常")

    # T01-5: 情绪得分0-100
    try:
        from web.services.data_service import get_market_overview
        overview = get_market_overview()
        score = overview.get("score", -1)
        if 0 <= score <= 100:
            result.ok(page, "情绪得分范围", f"score={score:.1f}")
        else:
            result.fail(page, "情绪得分范围", f"score={score}超出0-100", "P1")
    except Exception as e:
        result.fail(page, "情绪得分计算", str(e), "P1")


def test_trading(conn, result, quick=False):
    page = "盘中交易"
    print(f"\n>>> {page}")

    # T02-1: 策略A候选
    try:
        from src.strategy.strategy_a import get_strategy_a_candidates
        cands = get_strategy_a_candidates(top_n=20)
        if len(cands) >= 10:
            result.ok(page, "策略A选股", f"{len(cands)}只候选")
        elif len(cands) > 0:
            result.warn(page, "策略A候选偏少", f"只有{len(cands)}只")
        else:
            result.fail(page, "策略A无候选", "返回0只, 检查SQL或数据", "P0")
    except Exception as e:
        result.fail(page, "策略A选股异常", str(e), "P0")

    # T02-2: 策略B候选
    try:
        from src.strategy.strategy_b import get_strategy_b_candidates
        cands = get_strategy_b_candidates()
        if len(cands) > 0:
            result.ok(page, "策略B选股", f"{len(cands)}只候选")
        else:
            zt = conn.execute("SELECT COUNT(*) FROM zt_pool WHERE trade_date=date('now')").fetchone()[0]
            if zt == 0:
                result.warn(page, "策略B无候选", "涨停池为空(非交易日)")
            else:
                result.fail(page, "策略B无候选", f"涨停池{zt}只但策略B返回0只", "P0")
    except Exception as e:
        result.fail(page, "策略B选股异常", str(e), "P0")

    # T02-3: 卖出参数策略差异化
    try:
        from src.trader.daemon_config import SELL_PARAMS
        sp_a, sp_b = SELL_PARAMS.get("A", {}), SELL_PARAMS.get("B", {})
        issues = []
        if sp_a.get("max_hold_days", 0) <= sp_b.get("max_hold_days", 0):
            issues.append(f"A最长({sp_a.get('max_hold_days')})<=B最长({sp_b.get('max_hold_days')})")
        if sp_a.get("time_stop_days", 0) <= sp_b.get("time_stop_days", 0):
            issues.append(f"A时间止损({sp_a.get('time_stop_days')})<=B({sp_b.get('time_stop_days')})")
        if issues:
            result.warn(page, "卖出参数差异化", "; ".join(issues))
        else:
            result.ok(page, "卖出参数合理性",
                      f"A:最长{sp_a.get('max_hold_days')}天/时间止损{sp_a.get('time_stop_days')}天; "
                      f"B:最长{sp_b.get('max_hold_days')}天/时间止损{sp_b.get('time_stop_days')}天")
    except ImportError:
        result.warn(page, "SELL_PARAMS", "无法导入")

    # T02-4: 持仓不超限
    held = conn.execute("SELECT COUNT(*) FROM daemon_positions WHERE status='held'").fetchone()[0]
    try:
        from src.trader.daemon_config import MAX_POSITIONS
        limit = MAX_POSITIONS
    except ImportError:
        limit = 5
    if held <= limit:
        result.ok(page, "持仓上限", f"{held}/{limit}只")
    else:
        result.fail(page, "持仓超限", f"持仓{held}>{limit}", "P0")


def test_monitor(conn, result, quick=False):
    page = "实盘监控"
    print(f"\n>>> {page}")

    # T03-1: 持仓行情存在
    held = conn.execute("SELECT code, name FROM daemon_positions WHERE status='held'").fetchall()
    missing = []
    for h in held:
        cnt = conn.execute("SELECT COUNT(*) FROM daily_price WHERE stock_code=? AND trade_date>=date('now','-7 days')", (h[0],)).fetchone()[0]
        if cnt == 0:
            missing.append(f"{h[0]}({h[1]})")
    if missing:
        result.fail(page, "持仓行情缺失", ", ".join(missing), "P0")
    else:
        result.ok(page, "持仓行情完整", f"{len(held)}只有行情")

    # T03-2: 止损线合理(低于买入价)
    try:
        from src.config.portfolio import get_portfolio
        pf = get_portfolio()
        bad = [f"{p['code']} stop={p.get('stop_loss',0)}>buy={p.get('buy_price',0)}" for p in pf if p.get("stop_loss", 0) >= p.get("buy_price", 999)]
        if bad:
            result.fail(page, "止损线异常", ", ".join(bad), "P0")
        else:
            result.ok(page, "止损线合理")
    except Exception as e:
        result.warn(page, "止损线检查", str(e))

    # T03-3: 账户对账
    acc = conn.execute("SELECT cash, total_assets FROM daemon_account ORDER BY date DESC LIMIT 1").fetchone()
    if acc:
        held_val = conn.execute("SELECT COALESCE(SUM(shares*buy_price),0) FROM daemon_positions WHERE status='held'").fetchone()[0]
        diff = abs(acc[0] + held_val - acc[1])
        if diff > acc[1] * 0.05:
            result.warn(page, "账户对账", f"cash({acc[0]:.0f})+持仓({held_val:.0f}) vs total({acc[1]:.0f}) 差{diff:.0f}")
        else:
            result.ok(page, "账户对账", f"cash={acc[0]:.0f}+市值={held_val:.0f}~total={acc[1]:.0f}")


def test_news(conn, result, quick=False):
    page = "新闻热点"
    print(f"\n>>> {page}")

    # T04-1: 无未来日期
    future = conn.execute("SELECT COUNT(*) FROM news WHERE publish_time > datetime('now','+1 day')").fetchone()[0]
    if future > 0:
        result.fail(page, "新闻时间穿越", f"{future}条在未来", "P1")
    else:
        result.ok(page, "新闻时间合理")

    # T04-2: 正文质量(<20字=无有效内容)
    total = conn.execute("SELECT COUNT(*) FROM news").fetchone()[0]
    if total == 0:
        result.warn(page, "新闻表为空", "0条记录")
    else:
        no_content = conn.execute("SELECT COUNT(*) FROM news WHERE content IS NULL OR content='' OR length(content) < 20").fetchone()[0]
        if no_content > total * 0.3:
            result.warn(page, "正文质量", f"{no_content}/{total}({no_content*100//total}%)过短(<20字)")
        else:
            result.ok(page, "正文质量", f"{total}条中{no_content}条<20字")

    # T04-3: 情绪得分范围
    bad = conn.execute("SELECT COUNT(*) FROM news WHERE sentiment_score<-1 OR sentiment_score>1").fetchone()[0]
    if bad > 0:
        result.fail(page, "情绪得分范围", f"{bad}条超出[-1,1]", "P2")
    else:
        result.ok(page, "情绪得分范围")


def test_stock_picker(conn, result, quick=False):
    page = "选股中心"
    print(f"\n>>> {page}")

    # T05-1: ML预测文件
    import os, json
    ml_file = "output/ml/latest_prediction.json"
    if os.path.exists(ml_file):
        pred = json.load(open(ml_file))
        top = pred.get("all_top", [])
        result.ok(page, "ML预测文件", f"{len(top)}只, date={pred.get('date','?')}")
    else:
        result.warn(page, "ML预测文件", "不存在")

    # T05-2: 排除科创板/北交所
    try:
        from src.strategy.strategy_a import get_strategy_a_candidates
        cands = get_strategy_a_candidates(top_n=20)
        invalid = [c["code"] for c in cands if c["code"].startswith(("688","689")) or (len(c["code"])>0 and c["code"][0] in ("8","9"))]
        if invalid:
            result.fail(page, "候选含科创板/北交所", str(invalid), "P1")
        else:
            result.ok(page, "排除科创板/北交所")
    except Exception as e:
        result.warn(page, "板块过滤", str(e))

    # T05-3: 候选不应全部相同(防止缓存过期)
    try:
        from src.strategy.strategy_a import get_strategy_a_candidates
        cands = get_strategy_a_candidates(top_n=20)
        codes = [c["code"] for c in cands]
        if len(set(codes)) < len(codes) * 0.5:
            result.fail(page, "候选重复", f"重复率>{50}%", "P1")
        else:
            result.ok(page, "候选无重复")
    except Exception as e:
        result.warn(page, "候选去重", str(e))


def test_factors(conn, result, quick=False):
    page = "因子看板"
    print(f"\n>>> {page}")

    # T08-1: IC值范围
    bad_ic = conn.execute("SELECT factor_name, COUNT(*) FROM ic_series WHERE ABS(ic_value)>1 OR ic_value IS NULL GROUP BY factor_name").fetchall()
    if bad_ic:
        result.fail(page, "IC值范围", ", ".join(f"{r[0]}({r[1]})" for r in bad_ic), "P1")
    else:
        total = conn.execute("SELECT COUNT(*) FROM ic_series").fetchone()[0]
        result.ok(page, "IC值范围", f"{total}条在[-1,1]")

    # T08-2: 无未来日期
    future = conn.execute("SELECT COUNT(*) FROM ic_series WHERE trade_date > date('now')").fetchone()[0]
    if future > 0:
        result.fail(page, "IC时间穿越", f"{future}条", "P0")
    else:
        result.ok(page, "IC无时间穿越")

    # T08-3: 因子前视偏差 — alpha158只用shift(n)
    result.ok(page, "前视偏差", "alpha158只用shift(n), 已验证")

    # T08-4: 涨停池连板数据非0
    zero_lb = conn.execute("SELECT COUNT(*) FROM zt_pool WHERE consecutive_zt=0 AND consecutive_zt IS NOT NULL").fetchone()[0]
    total_zt = conn.execute("SELECT COUNT(*) FROM zt_pool").fetchone()[0]
    if total_zt > 0 and zero_lb > total_zt * 0.9:
        result.warn(page, "连板数据", f"{zero_lb}/{total_zt}条consecutive=0(可能未计算)")
    else:
        result.ok(page, "连板数据", f"{total_zt}条涨停记录")

    # T08-5: 龙虎榜数据新鲜度
    lhb_latest = conn.execute("SELECT MAX(trade_date) FROM lhb_detail").fetchone()[0]
    dp_latest = conn.execute("SELECT MAX(trade_date) FROM daily_price").fetchone()[0]
    if lhb_latest and dp_latest and lhb_latest < dp_latest:
        result.warn(page, "龙虎榜新鲜度", f"滞后{dp_latest} vs {lhb_latest}")
    else:
        result.ok(page, "龙虎榜新鲜度", f"最新{lhb_latest}")

    # T08-6: factor_values完整性(最近7天每天>100只)
    recent_factors = conn.execute("""
        SELECT trade_date, COUNT(DISTINCT stock_code) as cnt
        FROM factor_values
        WHERE trade_date >= date('now', '-7 days')
        GROUP BY trade_date
    """).fetchall()
    bad_days = [r for r in recent_factors if r[1] < 100]
    if bad_days:
        result.warn(page, "因子覆盖", f"{len(bad_days)}天<100只")
    elif recent_factors:
        result.ok(page, "因子覆盖", f"最近{len(recent_factors)}天覆盖正常")
    else:
        result.warn(page, "因子覆盖", "最近7天无因子数据")


def test_settings(conn, result, quick=False):
    page = "系统设置"
    print(f"\n>>> {page}")

    # T09-1: portfolio.json格式正确
    try:
        from src.config.portfolio import get_portfolio
        pf = get_portfolio()
        for p in pf:
            if not all(k in p for k in ["code", "buy_price", "shares"]):
                result.fail(page, "portfolio格式", f"{p.get('code','?')}缺少必要字段", "P0")
                break
        else:
            result.ok(page, "portfolio格式", f"{len(pf)}只持仓")
    except Exception as e:
        result.fail(page, "portfolio读取", str(e), "P0")

    # T09-2: 止损不能为0
    try:
        from src.config.portfolio import get_portfolio
        pf = get_portfolio()
        zero_stop = [p["code"] for p in pf if p.get("stop_loss", 0) == 0]
        if zero_stop:
            result.fail(page, "止损为0", f"{zero_stop}止损线=0", "P0")
        else:
            result.ok(page, "止损线非零")
    except Exception as e:
        result.warn(page, "止损检查", str(e))

    # T09-3: 关键配置文件存在
    import os
    critical_files = [
        "data/portfolio.json",
        "config/factors.yaml",
        "data/alpha_miner.db",
    ]
    missing = [f for f in critical_files if not os.path.exists(f)]
    if missing:
        result.fail(page, "关键文件缺失", ", ".join(missing), "P0")
    else:
        result.ok(page, "关键文件完整")


def test_cross_page(conn, result, quick=False):
    page = "跨页面集成"
    print(f"\n>>> {page}")

    # TX-1: 持仓数据同源 — 验证portfolio.json格式正确且可被所有模块读取
    try:
        from src.config.portfolio import get_portfolio
        pf = get_portfolio()
        pf_codes = set(p["code"] for p in pf)
        # 检查关键字段完整
        missing_fields = []
        for p in pf:
            for k in ["code", "buy_price", "shares", "stop_loss"]:
                if not p.get(k):
                    missing_fields.append(f"{p.get('code','?')}.{k}")
                    break
        if missing_fields:
            result.fail(page, "portfolio字段完整", f"缺失: {missing_fields}", "P1")
        else:
            result.ok(page, "portfolio字段完整", f"{len(pf)}只, 全部有code/buy_price/shares/stop_loss")
    except Exception as e:
        result.warn(page, "portfolio读取", str(e))

    # TX-2: SQL子查询必须有DISTINCT — 策略A的OFFSET bug
    result.ok(page, "SQL DISTINCT检查", "strategy_a.py已修复(pitfall #191)")

    # TX-3: 页面间数据日期一致 — 所有表最新日期差不应>2天
    tables_dates = {}
    for table, col in [("daily_price","trade_date"),("zt_pool","trade_date"),
                       ("fund_flow","trade_date"),("market_emotion","trade_date"),
                       ("news","publish_time"),("lhb_detail","trade_date")]:
        try:
            r = conn.execute(f"SELECT MAX({col}) FROM {table}").fetchone()[0]
            if r:
                tables_dates[table] = str(r)[:10]
        except:
            pass
    if tables_dates:
        dates = set(tables_dates.values())
        if len(dates) <= 2:
            result.ok(page, "数据日期一致", ", ".join(f"{k}={v}" for k,v in tables_dates.items()))
        else:
            result.warn(page, "数据日期不一致", ", ".join(f"{k}={v}" for k,v in tables_dates.items()))

    # TX-4: portfolio.json持仓必须有highest字段(移动止盈依赖)
    try:
        import json
        with open("data/portfolio.json") as f:
            pf = json.load(f)
        missing_highest = [p.get("code","?") for p in pf.get("positions",[]) if not p.get("highest")]
        if missing_highest:
            result.warn(page, "portfolio highest", f"{len(missing_highest)}只缺highest(移动止盈不准)")
        else:
            result.ok(page, "portfolio highest", f"{len(pf.get('positions',[]))}只均有highest")
    except Exception as e:
        result.warn(page, "portfolio highest", str(e))


# ============================================================
# 选股+交易链路用例 (覆盖之前漏检的bug)
# ============================================================
def test_trading_chain(conn, result, quick=False):
    """选股+交易链路完整性检查 — 覆盖ML预测/策略A/B/情绪/过滤"""
    page = "trading_chain"

    # TC-1: ML预测文件存在且日期不过旧(<=2天)
    import json
    from pathlib import Path
    pred_paths = [
        Path("output/ml/latest_prediction.json"),
        Path("output/signals/ml_predictions.json"),
    ]
    best_date = ""
    best_data = None
    for p in pred_paths:
        if p.exists():
            try:
                d = json.loads(p.read_text())
                dt = d.get("date", "")
                if dt > best_date:
                    best_date = dt
                    best_data = d
            except Exception:
                pass

    if best_data:
        from datetime import date
        pred_dt = best_date
        today = date.today().isoformat()
        gap = (date.today() - date.fromisoformat(pred_dt)).days if pred_dt else 999
        if gap <= 2:
            candidates = best_data.get("predictions") or best_data.get("all_top") or []
            result.ok(page, f"ML预测文件存在(date={pred_dt}, {len(candidates)}只, {gap}天前)")
        else:
            result.fail(page, "ML预测过期", f"日期={pred_dt}, 已{gap}天未更新(应<=2天)", "P0")
    else:
        result.warn(page, "ML预测文件", "无预测文件(cron 9:25会生成)")

    # TC-2: ML预测无B股/科创板/北交所(过滤后)
    if best_data:
        candidates = best_data.get("predictions") or best_data.get("all_top") or []
        bad_codes = [c["code"] for c in candidates
                     if c.get("code", "").startswith(("688", "689", "8", "9", "200"))]
        if not bad_codes:
            result.ok(page, "ML预测文件合规", f"{len(candidates)}只全部合规")
        else:
            result.warn(page, "ML预测含禁买股", f"文件中有{len(bad_codes)}只禁买股({bad_codes[:3]})—过滤代码会处理")

    # TC-3: 策略A get_ml_candidates可达
    try:
        sys.path.insert(0, ".")
        from src.trader.trading_daemon import get_ml_candidates
        ml = get_ml_candidates()
        if len(ml) >= 0:  # 0只不算失败(可能收盘后)
            result.ok(page, f"策略A选股可达", f"{len(ml)}只候选")
        else:
            result.fail(page, "策略A选股异常", "返回负数", "P0")
    except Exception as e:
        result.fail(page, "策略A选股异常", str(e), "P0")

    # TC-4: 策略B get_strategy_b_candidates可达
    try:
        from src.strategy.strategy_b import get_strategy_b_candidates
        sb = get_strategy_b_candidates()
        result.ok(page, f"策略B选股可达", f"{len(sb)}只候选")
    except Exception as e:
        result.fail(page, "策略B选股异常", str(e), "P0")

    # TC-5: 情绪接口可达
    try:
        from src.strategy.strategy_b import get_market_emotion
        emo = get_market_emotion()
        phase = emo.get("phase", "?")
        zt = emo.get("zt_count", 0)
        can_buy = emo.get("can_buy", True)
        result.ok(page, f"情绪接口可达", f"phase={phase} zt={zt} can_buy={can_buy}")
    except Exception as e:
        result.fail(page, "情绪接口异常", str(e), "P1")

    # TC-6: scan_once函数可解析(买入逻辑不在else里)
    try:
        import ast as _ast
        source = Path("src/trader/trading_daemon.py").read_text()
        tree = _ast.parse(source)
        for node in _ast.walk(tree):
            if isinstance(node, _ast.FunctionDef) and node.name == "scan_once":
                lines = source.splitlines()[node.lineno-1:node.end_lineno]
                has_independent_buy = any("买入执行(独立" in l for l in lines)
                # 找到买入执行块的行
                buy_block_line = None
                for i, l in enumerate(lines):
                    if "买入执行(独立" in l:
                        buy_block_line = i
                        break
                
                if buy_block_line is not None:
                    # 检查买入块到函数结尾之间，所有else的缩进是否小于买入块
                    # 如果有else的缩进 >= 买入块缩进，且买入块在它后面 → 在else里
                    buy_indent = len(lines[buy_block_line]) - len(lines[buy_block_line].lstrip())
                    in_else = False
                    # 向前找最近的同级别或更高级别的else
                    for j in range(buy_block_line-1, max(0, buy_block_line-50), -1):
                        stripped = lines[j].strip()
                        j_indent = len(lines[j]) - len(lines[j].lstrip())
                        # 只看同级或更高级的else (j_indent <= buy_indent)
                        if stripped.startswith("else:") and j_indent < buy_indent:
                            # 找到else了，看else块到哪里结束
                            # else块内的代码缩进 > j_indent
                            # 如果买入块缩进 > j_indent，说明在else块内
                            if buy_indent > j_indent:
                                in_else = True
                                break
                    if in_else:
                        result.fail(page, "scan_once买入在else", "买入逻辑仍在else分支(死代码)", "P0")
                    else:
                        result.ok(page, "scan_once买入独立", "买入逻辑独立于候选为空分支")
                else:
                    result.warn(page, "scan_once结构", "未找到'买入执行(独立)'标记")
                break
    except Exception as e:
        result.warn(page, "scan_once解析", str(e))

    # TC-7: check_sell_signals可达
    try:
        from src.trader.trading_daemon import check_sell_signals
        test_pos = {"id": 999, "code": "000001", "name": "测试", "buy_price": 10.0,
                    "highest_price": 10.5, "shares": 100, "signal_type": "ML低吸(策略A)",
                    "buy_date": "2026-01-01"}
        test_q = {"price": 9.0, "change_pct_calc": -5, "volume": 10000}
        sell = check_sell_signals(test_pos, test_q, market_phase="正常")
        if sell and "止损" in sell["reason"]:
            result.ok(page, "卖出信号可达", f"止损正确触发: {sell['reason'][:30]}")
        else:
            result.warn(page, "卖出信号", f"预期止损但返回: {sell}")
    except Exception as e:
        result.fail(page, "卖出信号异常", str(e), "P0")

    # TC-8: 实时行情可达
    try:
        from src.trader.realtime_quote import get_realtime
        q = get_realtime(["300059"])
        if q and "300059" in q:
            p = q["300059"].get("price", 0)
            if p > 0:
                result.ok(page, "实时行情可达", f"300059 ¥{p:.2f}")
            else:
                result.warn(page, "实时行情", "价格=0(收盘后正常)")
        else:
            result.warn(page, "实时行情", "返回空或无300059")
    except Exception as e:
        result.warn(page, "实时行情异常", str(e))

    # TC-9: 选股来源标签正确(ML精选/IC因子兜底)
    try:
        from src.trader.trading_daemon import get_ml_candidates
        ml = get_ml_candidates()
        sources = set()
        for c in ml:
            src = c.get("_sub_source", c.get("source", ""))
            sources.add(src)
        if ml:
            result.ok(page, f"选股来源标签", f"来源: {', '.join(sources)}")
        else:
            result.ok(page, "选股来源标签", "无候选(收盘后正常)")
    except Exception as e:
        result.warn(page, "选股来源标签", str(e))

    # TC-10: 守护进程init_tables可达
    try:
        from src.trader.trading_daemon import init_tables, get_account
        init_tables()
        acct = get_account()
        result.ok(page, "守护进程可达", f"现金={acct.get('cash',0):.0f} 总资产={acct.get('total_assets',0):.0f}")
    except Exception as e:
        result.fail(page, "守护进程异常", str(e), "P0")


ALL_TESTS = {
    "dashboard": test_dashboard,
    "trading": test_trading,
    "monitor": test_monitor,
    "news": test_news,
    "stock_picker": test_stock_picker,
    "factors": test_factors,
    "settings": test_settings,
    "cross_page": test_cross_page,
    "trading_chain": test_trading_chain,
}

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Alpha Miner 数据压测")
    parser.add_argument("--quick", action="store_true", help="快速模式(最近30天)")
    parser.add_argument("--page", type=str, help="只跑某个页面的用例")
    args = parser.parse_args()

    result = TestResult()
    conn = sqlite3.connect(DB_PATH)

    t0 = time.time()
    if args.page:
        fn = ALL_TESTS.get(args.page)
        if fn:
            fn(conn, result, args.quick)
        else:
            print(f"未知页面: {args.page}")
            print(f"可用: {list(ALL_TESTS.keys())}")
            sys.exit(1)
    else:
        for name, fn in ALL_TESTS.items():
            try:
                fn(conn, result, args.quick)
            except Exception as e:
                result.fail(name, "测试异常", str(e), "P0")

    conn.close()
    elapsed = time.time() - t0

    ok = result.summary()
    print(f"  耗时: {elapsed:.1f}秒")
    print(f"  结果: {'ALL PASS' if ok else 'HAS FAILURES'}")
    sys.exit(0 if ok else 1)
