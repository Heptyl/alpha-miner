"""
统一策略注册表 — trader层通过此模块调用策略，不直接import具体策略文件。

设计原则:
  - 每个策略提供统一接口: scan() -> list[dict] 候选列表
  - trader层只调用 StrategyRegistry.get_candidates(strategy_name)
  - 策略文件可以自由重构内部实现，只要scan()接口不变

解耦效果:
  之前: trading_daemon → from src.strategy.strategy_a import get_strategy_a_candidates
        trading_daemon → from src.strategy.strategy_b import get_strategy_b_candidates
        strategy_b → from src.strategy.dragon_score import dragon_score
  之后: trading_daemon → from src.strategy.registry import StrategyRegistry
        strategy_b内部依赖dragon_score (策略内部,可接受)
"""

import logging
from typing import Callable, Optional

logger = logging.getLogger(__name__)


class StrategyRegistry:
    """
    策略注册表 — 统一管理所有策略的候选扫描接口。
    
    用法:
      # 注册策略(在策略文件底部调用)
      StrategyRegistry.register("A", scan_func=my_scan_a)
      
      # 获取候选(在daemon中调用)
      candidates = StrategyRegistry.get_candidates("A")
    """
    
    _strategies: dict[str, Callable[..., list[dict]]] = {}
    _initialized: bool = False
    
    @classmethod
    def register(cls, name: str, scan_func: Callable[[], list[dict]]) -> None:
        """注册策略的候选扫描函数"""
        cls._strategies[name] = scan_func
        logger.debug(f"[Registry] 策略{name}已注册: {scan_func.__module__}.{scan_func.__name__}")
    
    @classmethod
    def get_candidates(cls, name: str) -> list[dict]:
        """
        获取指定策略的候选列表。
        
        首次调用时自动触发所有策略的注册(lazy init)。
        策略加载失败返回空列表，不影响其他策略。
        """
        if not cls._initialized:
            cls._auto_register()
        
        func = cls._strategies.get(name)
        if func is None:
            logger.debug(f"[Registry] 策略{name}未注册，返回空候选")
            return []
        
        try:
            return func()
        except Exception as e:
            logger.warning(f"[Registry] 策略{name}候选生成失败: {e}")
            return []
    
    @classmethod
    def _auto_register(cls) -> None:
        """自动加载并注册所有策略(延迟import避免循环依赖)"""
        # 策略A: 龙头首阴反包
        try:
            from src.strategy.strategy_a import get_strategy_a_candidates
            cls.register("A", get_strategy_a_candidates)
        except Exception as e:
            logger.warning(f"[Registry] 策略A加载失败: {e}")
        
        # 策略B: 已从Registry移除 — B_crash_v2候选由daemon_strategies._get_b_watchlist()产生
        # Registry.get_candidates("B") 现在返回空列表
        # 旧低开反弹保留为B_LOW_OPEN_V1(status=pause), 仅供回测参考, 不进daemon
        try:
            from src.strategy.strategy_b import get_strategy_b_candidates
            cls.register("B_LOW_OPEN_V1", get_strategy_b_candidates)
        except Exception as e:
            logger.warning(f"[Registry] 策略B_LOW_OPEN_V1加载失败: {e}")
        
        # 策略C: 趋势牛股
        try:
            from src.strategy.strategy_c_v2 import get_strategy_c_v2_candidates
            cls.register("C", get_strategy_c_v2_candidates)
        except Exception as e:
            logger.warning(f"[Registry] 策略C加载失败: {e}")

        # 策略C1/C2: shadow-only研究假设
        try:
            from src.strategy.strategy_c_shadow import (
                get_c1_attention_momentum_candidates,
                get_c2_panic_reversal_candidates,
            )
            cls.register("C1", get_c1_attention_momentum_candidates)
            cls.register("C2", get_c2_panic_reversal_candidates)
        except Exception as e:
            logger.warning(f"[Registry] 策略C1/C2加载失败: {e}")
        
        cls._initialized = True
        registered = list(cls._strategies.keys())
        logger.info(f"[Registry] 策略注册完成: {registered}")
    
    @classmethod
    def reset(cls) -> None:
        """重置注册表(测试用)"""
        cls._strategies = {}
        cls._initialized = False
    
    @classmethod
    def list_registered(cls) -> list[str]:
        """列出已注册的策略名"""
        return list(cls._strategies.keys())
