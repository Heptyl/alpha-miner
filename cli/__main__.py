"""Alpha Miner CLI entrypoint with one default USER path."""

import sys


def _configure_non_tty_utf8() -> None:
    """Make redirected/agent output deterministic without changing a real console."""
    for stream in (sys.stdout, sys.stderr):
        if not stream.isatty() and hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8")


def _print_user_help() -> None:
    print("Usage: python -m cli [command] [args]")
    print()
    print("USER commands:")
    print("  play                         日常主入口（无参数时默认执行）")
    print("  zt status                    详细数据诊断")
    print("  report --brief --holdings    持仓检查")
    print()
    print("其他命令属于RD/后台流程，仍兼容但不在USER帮助中公开。")

if __name__ == "__main__":
    _configure_non_tty_utf8()

    sub = sys.argv[1] if len(sys.argv) > 1 else "play"

    if sub in ("help", "--help", "-h"):
        _print_user_help()
        raise SystemExit(0)

    # Strip subcommand from argv so argparse in sub-modules works correctly
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
    elif sub == "daily":
        from cli.daily import main
        main()
    elif sub == "strategy":
        from cli.strategy import main
        main()
    elif sub in ("limit-up", "zt"):
        from cli.limit_up import main
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
    elif sub == "play":
        # USER read-only entry must not require writable bytecode caches.
        sys.dont_write_bytecode = True
        from cli.play import main
        main()
    else:
        print(f"Unknown command: {sub}", file=sys.stderr)
        print("Run 'python -m cli --help' for USER commands.", file=sys.stderr)
        raise SystemExit(2)
