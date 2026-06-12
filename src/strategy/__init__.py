# 策略层公共接口
#
# 所有策略通过 registry 统一注册，trader层不直接import具体策略文件。
# 调用方式: StrategyRegistry.get_candidates("A") / StrategyRegistry.get_candidates("B")
