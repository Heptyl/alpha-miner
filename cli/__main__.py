"""Alpha Miner CLI 入口。"""

if __name__ == "__main__":
    import sys
    # python -m cli.collect → 路由到 cli.collect:main
    # python -m cli → 显示帮助
    from cli import collect
    collect.main()
