"""Alpha Miner CLI 入口。

python -m cli collect [args]
python -m cli report [args]
python -m cli mine [args]
python -m cli drift [args]
python -m cli backtest [args]
"""

if __name__ == "__main__":
    import sys

    sub = sys.argv[1] if len(sys.argv) > 1 else "help"

    # Strip subcommand from argv so argparse in sub-modules works correctly
    if sub not in ("help",):
        sys.argv = [sys.argv[0]] + sys.argv[2:]

    if sub == "collect":
        from cli.collect import main
        main()
    elif sub == "report":
        from cli.report import main
        main()
    elif sub in ("mine", "evolve"):
        from cli.mine import main
        main()
    elif sub == "drift":
        from cli.drift import main
        main()
    elif sub == "backtest":
        from cli.backtest import main
        main()
    elif sub == "script":
        from cli.report import main_script
        main_script()
    elif sub == "replay":
        from cli.replay import main
        main()
    elif sub == "strategy":
        from cli.strategy import main
        main()
    elif sub == "signal":
        from cli.signal import main
        main()
    elif sub == "query":
        from cli.query import main
        main()
    elif sub == "recommend":
        from cli.recommend import main
        main()
    elif sub == "help":
        print("Usage: python -m cli <command> [args]")
        print()
        print("Commands:")
        print("  collect   采集数据")
        print("  report    生成日报/盘后简报 (--brief)")
        print("  mine      因子进化挖掘")
        print("  drift     漂移检测")
        print("  backtest  回测")
        print("  script    生成市场剧本")
        print("  replay    复盘昨日剧本")
        print("  strategy  策略管理 (list/backtest/evolve/scan)")
        print("  signal    次日选股信号")
        print("  recommend 每日个股推荐(含买入点位)")
        print("  query     查询股票数据/市场概览")
    else:
        print(f"Unknown command: {sub}")
        sys.exit(1)
