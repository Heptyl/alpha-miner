#!/usr/bin/env python3
"""每日自动采集+压测 — 由cron触发

流程:
1. 采集当天数据(日K/涨停/炸板/龙虎榜/资金流/新闻)
2. 跑压测(32用例)
3. 输出报告到stdout(cron会发到用户)

不用Hermes agent, 直接python跑, 快速稳定。
"""
import subprocess, sys, os, json
from datetime import datetime

os.chdir("/home/ccy/alpha-miner")

def run(cmd, timeout=300):
    """运行命令, 返回stdout"""
    try:
        r = subprocess.run(
            cmd, shell=True, capture_output=True, text=True,
            timeout=timeout, env={**os.environ, "PYTHONPATH": "/home/ccy/alpha-miner"}
        )
        return r.stdout.strip(), r.returncode
    except subprocess.TimeoutExpired:
        return f"TIMEOUT({timeout}s)", -1

def main():
    now = datetime.now()
    weekday = now.weekday()
    
    print(f"{'='*50}")
    print(f"  Alpha Miner 每日采集+压测")
    print(f"  {now.strftime('%Y-%m-%d %H:%M')} (周{'一二三四五六日'[weekday]})")
    print(f"{'='*50}")
    
    # 周末跳过
    if weekday >= 5:
        print("\n⚠️ 周末非交易日, 跳过采集")
        return
    
    # Step 1: 采集
    print("\n[1/2] 数据采集...")
    out, rc = run("uv run python -m cli.collect --today", timeout=300)
    if rc == 0:
        # 提取关键信息
        for line in out.split("\n"):
            if any(k in line for k in ["✅", "❌", "完成", "失败", "Error", "条"]):
                print(f"  {line.strip()}")
    else:
        print(f"  ❌ 采集失败 (rc={rc})")
        print(f"  {out[-200:]}" if len(out) > 200 else f"  {out}")
    
    # Step 2: 压测
    print("\n[2/2] 压测检查...")
    out, rc = run("uv run python tests/test_data_integrity.py", timeout=120)
    
    # 解析结果
    total = pass_count = fail_count = warn_count = 0
    for line in out.split("\n"):
        if "通过" in line and "失败" in line:
            print(f"  {line.strip()}")
            # 尝试解析数字
            import re
            nums = re.findall(r'(\d+)', line)
            if len(nums) >= 3:
                total, pass_count, fail_count = int(nums[0]), int(nums[0]), int(nums[1])
    
    # 输出失败/警告的详情
    failures = []
    for line in out.split("\n"):
        if "[FAIL]" in line or "❌" in line:
            failures.append(line.strip())
    
    if failures:
        print("\n  ⚠️ 失败项:")
        for f in failures[:5]:
            print(f"    {f}")
    
    # Step 3: 模拟盘日报
    print("\n[3/3] 模拟盘日报...")
    try:
        import sqlite3
        conn = sqlite3.connect("data/alpha_miner.db")
        today = now.strftime("%Y-%m-%d")
        
        # 账户
        acct = conn.execute("SELECT cash, total_assets, daily_pnl FROM daemon_account WHERE date=? ORDER BY date DESC LIMIT 1", (today,)).fetchone()
        if acct:
            pnl_sign = "+" if acct[2] >= 0 else ""
            print(f"  总资产: ¥{acct[1]:,.0f} | 现金: ¥{acct[0]:,.0f} | 今日: {pnl_sign}¥{acct[2]:,.0f}")
        
        # 持仓
        held = conn.execute("SELECT code, name, buy_price, signal_type FROM daemon_positions WHERE status='held'").fetchall()
        if held:
            print(f"  持仓: {len(held)}只")
            for h in held:
                print(f"    {h[1]}({h[0]}) @{h[2]:.2f} [{h[3]}]")
        else:
            print("  持仓: 空")
        
        # 今日交易
        trades = conn.execute("SELECT action, code, name, price, shares, reason FROM daemon_trades WHERE trade_date=?", (today,)).fetchall()
        if trades:
            print(f"  今日交易: {len(trades)}笔")
            for t in trades:
                icon = "🟢" if t[0] == "buy" else "🔴"
                print(f"    {icon} {t[1]} {t[2]} @{t[3]:.2f}x{t[4]} {t[5][:30]}")
        else:
            print("  今日无交易")
        
        conn.close()
    except Exception as e:
        print(f"  日报异常: {e}")
    
    print(f"\n{'='*50}")
    print(f"  完成 {now.strftime('%H:%M')}")
    print(f"{'='*50}")

if __name__ == "__main__":
    main()
