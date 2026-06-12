"""llm_client.py — 统一LLM客户端

统一管理所有LLM调用:
  - 多provider: GLM-5.1(智谱) / DeepSeek / OpenAI兼容
  - 统一重试/超时/fallback逻辑
  - 调用统计(次数/Token/成本/失败率)写入llm_usage表
  - 全局单例 get_client()

调用方:
  src/strategy/llm_analysis.py — 策略分析
  src/agent/sentiment_analyzer.py — 情感分析
  src/agent/review_agent.py — 盘后复盘
  src/narrative/*.py — 叙事生成

用法:
  from src.agent.llm_client import get_client
  client = get_client()
  text = client.chat("分析这只股票...", caller="strategy_a")
"""

from __future__ import annotations

import logging
import os
import sqlite3
import time
import threading
from datetime import datetime, date
from pathlib import Path
from typing import Optional

logger = logging.getLogger("llm_client")

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_ROOT / "data" / "alpha_miner.db"
CONFIG_PATH = PROJECT_ROOT / "config" / "settings.yaml"

# ── Token成本估算(元/千Token) ──
COST_TABLE = {
    "glm-5.1": {"input": 0.005, "output": 0.005},
    "glm-4-plus": {"input": 0.05, "output": 0.05},
    "glm-4-flash": {"input": 0.001, "output": 0.001},
    "deepseek-v4-flash": {"input": 0.001, "output": 0.002},
    "deepseek-chat": {"input": 0.001, "output": 0.002},
    "claude-sonnet-4-6": {"input": 0.021, "output": 0.105},
    "gpt-4o-mini": {"input": 0.0105, "output": 0.042},
}

# ── 默认参数 ──
DEFAULT_TIMEOUT = 120
DEFAULT_RETRIES = 3
RETRY_DELAY_BASE = 5  # 指数退避基数(秒): 5 → 15 → 30


def _load_yaml_config() -> dict:
    """加载config/settings.yaml中的api配置"""
    try:
        import yaml
        if CONFIG_PATH.exists():
            with open(CONFIG_PATH, encoding="utf-8") as f:
                cfg = yaml.safe_load(f) or {}
            return cfg.get("api", {})
    except Exception:
        pass
    return {}


class LLMClient:
    """统一LLM客户端 — 多provider + 重试 + 统计"""

    def __init__(self):
        self._cfg = _load_yaml_config()
        self._providers = self._build_providers()
        # 运行时状态
        self._zhipu_key_index = 0
        self._zhipu_working_key = None
        # 统计(内存累计, 定期刷盘)
        self._stats_lock = threading.Lock()
        self._stats: list[dict] = []
        self._flush_interval = 60  # 秒
        self._last_flush = time.time()

    def _build_providers(self) -> list[dict]:
        """构建provider列表(按优先级)"""
        providers = []

        # 1. DeepSeek
        ds_key = os.environ.get("DEEPSEEK_API_KEY", "") or \
                 self._cfg.get("deepseek", {}).get("api_key", "")
        if ds_key and ds_key not in ("YOUR_KEY_HERE", ""):
            ds_cfg = self._cfg.get("deepseek", {})
            providers.append({
                "name": "deepseek",
                "api_key": ds_key,
                "base_url": os.environ.get("DEEPSEEK_BASE_URL",
                                           ds_cfg.get("base_url", "https://api.deepseek.com/")),
                "model": ds_cfg.get("model", "deepseek-v4-flash"),
                "type": "openai_compat",
            })

        # 2. 智谱GLM
        zhipu_cfg = self._cfg.get("zhipu", {})
        zhipu_keys = zhipu_cfg.get("api_keys", [])
        if not zhipu_keys:
            single_key = os.environ.get("ZAI_API_KEY", "") or zhipu_cfg.get("api_key", "")
            if single_key and single_key not in ("YOUR_KEY_HERE", ""):
                zhipu_keys = [single_key]
        if zhipu_keys:
            providers.append({
                "name": "zhipu",
                "api_keys": zhipu_keys,
                "base_url": os.environ.get("ZAI_BASE_URL",
                                           zhipu_cfg.get("base_url", "https://open.bigmodel.cn/api/anthropic")),
                "model": zhipu_cfg.get("model", "glm-5.1"),
                "type": "anthropic",
            })

        # 3. OpenAI兼容
        openai_key = os.environ.get("OPENAI_API_KEY", "") or \
                     self._cfg.get("openai", {}).get("api_key", "")
        if openai_key and openai_key not in ("YOUR_KEY_HERE", ""):
            oi_cfg = self._cfg.get("openai", {})
            providers.append({
                "name": "openai",
                "api_key": openai_key,
                "base_url": os.environ.get("OPENAI_BASE_URL",
                                           oi_cfg.get("base_url", "https://api.openai.com/v1/")),
                "model": oi_cfg.get("model", "gpt-4o-mini"),
                "type": "anthropic",
            })

        # 4. 通用LLM_API_KEY
        any_key = os.environ.get("LLM_API_KEY", "")
        any_url = os.environ.get("LLM_BASE_URL", "")
        if any_key and any_url:
            providers.append({
                "name": "generic",
                "api_key": any_key,
                "base_url": any_url,
                "model": os.environ.get("LLM_MODEL", "gpt-4o-mini"),
                "type": "openai_compat",
            })

        return providers

    @property
    def has_provider(self) -> bool:
        return len(self._providers) > 0

    @property
    def primary_model(self) -> str:
        """返回首选provider的模型名"""
        if self._providers:
            return self._providers[0].get("model", "unknown")
        return "none"

    def chat(self, prompt: str, *, system: str = "",
             max_tokens: int = 4000, temperature: float = 0.3,
             caller: str = "", retries: int = DEFAULT_RETRIES,
             timeout: int = DEFAULT_TIMEOUT) -> Optional[str]:
        """统一对话接口 — 自动选择provider, 带重试和fallback

        Args:
            prompt: 用户提示词
            system: 系统提示词(可选)
            max_tokens: 最大输出Token
            temperature: 温度
            caller: 调用方标识(用于统计)
            retries: 重试次数
            timeout: 超时秒数

        Returns:
            LLM回复文本, 失败返回None
        """
        if not self._providers:
            logger.warning("LLM无可用provider, 请配置API Key")
            return None

        last_error = None
        for provider in self._providers:
            result = self._call_provider(
                provider, prompt, system, max_tokens, temperature,
                caller, retries, timeout,
            )
            if result is not None:
                return result
            # provider失败, 尝试下一个

        logger.warning("所有LLM provider均失败, last_error=%s", last_error)
        return None

    def get_anthropic_client(self) -> tuple[Optional[object], str]:
        """获取Anthropic SDK client(供需要流式/高级功能的模块使用)

        Returns:
            (client, model) 或 (None, "")
        """
        for p in self._providers:
            if p["type"] == "anthropic":
                import anthropic
                keys = p.get("api_keys", [p.get("api_key", "")])
                if keys:
                    client = anthropic.Anthropic(api_key=keys[0], base_url=p["base_url"])
                    return client, p["model"]
        return None, ""

    def get_stats_summary(self) -> dict:
        """获取当前统计摘要"""
        with self._stats_lock:
            calls = len(self._stats)
            failures = sum(1 for s in self._stats if not s.get("success"))
            total_cost = sum(s.get("cost", 0) for s in self._stats)
            total_latency = sum(s.get("latency_ms", 0) for s in self._stats)
            return {
                "calls": calls,
                "failures": failures,
                "failure_rate": failures / calls if calls else 0,
                "total_cost": round(total_cost, 4),
                "avg_latency_ms": round(total_latency / calls, 0) if calls else 0,
            }

    def flush_stats(self):
        """将内存统计写入DB"""
        with self._stats_lock:
            if not self._stats:
                return
            batch = self._stats[:]
            self._stats.clear()

        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute("""
                CREATE TABLE IF NOT EXISTS llm_usage (
                    id           INTEGER PRIMARY KEY AUTOINCREMENT,
                    trade_date   TEXT NOT NULL,
                    caller       TEXT DEFAULT '',
                    provider     TEXT DEFAULT '',
                    model        TEXT DEFAULT '',
                    success      INTEGER DEFAULT 0,
                    latency_ms   INTEGER DEFAULT 0,
                    input_tokens INTEGER DEFAULT 0,
                    output_tokens INTEGER DEFAULT 0,
                    cost         REAL DEFAULT 0,
                    error        TEXT DEFAULT '',
                    created_at   TEXT DEFAULT (datetime('now'))
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_usage_date ON llm_usage(trade_date)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_llm_usage_caller ON llm_usage(caller)")

            for s in batch:
                conn.execute("""
                    INSERT INTO llm_usage (trade_date, caller, provider, model, success,
                                           latency_ms, input_tokens, output_tokens, cost, error)
                    VALUES (?,?,?,?,?,?,?,?,?,?)
                """, (
                    s.get("date", date.today().isoformat()),
                    s.get("caller", ""),
                    s.get("provider", ""),
                    s.get("model", ""),
                    1 if s.get("success") else 0,
                    s.get("latency_ms", 0),
                    s.get("input_tokens", 0),
                    s.get("output_tokens", 0),
                    s.get("cost", 0),
                    s.get("error", "")[:200],
                ))
            conn.commit()
            conn.close()
            logger.debug("[llm_client] 刷盘%d条统计", len(batch))
        except Exception as e:
            logger.debug("[llm_client] 统计刷盘失败: %s", e)

    # ── 内部方法 ──

    def _record(self, caller: str, provider: str, model: str,
                success: bool, latency_ms: int, input_tokens: int = 0,
                output_tokens: int = 0, cost: float = 0, error: str = ""):
        """记录一次调用统计"""
        with self._stats_lock:
            self._stats.append({
                "date": date.today().isoformat(),
                "caller": caller,
                "provider": provider,
                "model": model,
                "success": success,
                "latency_ms": latency_ms,
                "input_tokens": input_tokens,
                "output_tokens": output_tokens,
                "cost": cost,
                "error": error,
            })
            # 定期刷盘
            if time.time() - self._last_flush > self._flush_interval or len(self._stats) >= 50:
                self._last_flush = time.time()
                self.flush_stats()

    def _call_provider(self, provider: dict, prompt: str, system: str,
                       max_tokens: int, temperature: float,
                       caller: str, retries: int, timeout: int) -> Optional[str]:
        """调用单个provider"""
        ptype = provider["type"]
        pname = provider["name"]

        if ptype == "openai_compat":
            return self._call_openai_compat(provider, prompt, system,
                                            max_tokens, temperature,
                                            caller, retries, timeout)
        elif ptype == "anthropic":
            return self._call_anthropic(provider, prompt, system,
                                        max_tokens, temperature,
                                        caller, retries, timeout)
        return None

    def _call_openai_compat(self, provider: dict, prompt: str, system: str,
                            max_tokens: int, temperature: float,
                            caller: str, retries: int, timeout: int) -> Optional[str]:
        """调用OpenAI兼容接口(DeepSeek等)"""
        import requests as req

        base_url = provider["base_url"].rstrip("/") + "/"
        model = provider["model"]

        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})

        for attempt in range(retries):
            t0 = time.time()
            try:
                resp = req.post(
                    f"{base_url}chat/completions",
                    headers={
                        "Authorization": f"Bearer {provider['api_key']}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": model,
                        "max_tokens": max_tokens,
                        "temperature": temperature,
                        "messages": messages,
                    },
                    timeout=timeout,
                )
                resp.raise_for_status()
                data = resp.json()
                content = data["choices"][0]["message"].get("content", "")

                # 推理模型(v4-flash)可能有reasoning_content
                if not content:
                    reasoning = data["choices"][0]["message"].get("reasoning_content", "")
                    if reasoning:
                        content = reasoning.split("\n")[-1].strip()

                latency = int((time.time() - t0) * 1000)
                usage = data.get("usage", {})
                in_tok = usage.get("prompt_tokens", 0)
                out_tok = usage.get("completion_tokens", 0)
                cost = self._estimate_cost(model, in_tok, out_tok)
                self._record(caller, provider["name"], model, True,
                             latency, in_tok, out_tok, cost)
                return content if content else None

            except Exception as e:
                latency = int((time.time() - t0) * 1000)
                self._record(caller, provider["name"], model, False,
                             latency, error=str(e)[:200])
                if attempt < retries - 1:
                    time.sleep(RETRY_DELAY_BASE * (3 ** attempt))
                else:
                    logger.warning("[%s] %s调用失败(重试%d次): %s",
                                   caller, provider["name"], retries, str(e)[:100])
        return None

    def _call_anthropic(self, provider: dict, prompt: str, system: str,
                        max_tokens: int, temperature: float,
                        caller: str, retries: int, timeout: int) -> Optional[str]:
        """调用Anthropic SDK兼容接口(智谱GLM等)"""
        import anthropic

        base_url = provider["base_url"]
        model = provider["model"]
        keys = provider.get("api_keys", [provider.get("api_key", "")])
        if not keys:
            return None

        # 快速路径: 用上次成功的key
        if self._zhipu_working_key and self._zhipu_working_key in keys:
            t0 = time.time()
            try:
                client = anthropic.Anthropic(api_key=self._zhipu_working_key, base_url=base_url)
                kwargs = {
                    "model": model, "max_tokens": max_tokens,
                    "temperature": temperature,
                    "messages": [{"role": "user", "content": prompt}],
                }
                if system:
                    kwargs["system"] = system
                message = client.messages.create(**kwargs)
                content = message.content[0].text
                latency = int((time.time() - t0) * 1000)
                in_tok = getattr(message.usage, "input_tokens", 0) or 0
                out_tok = getattr(message.usage, "output_tokens", 0) or 0
                cost = self._estimate_cost(model, in_tok, out_tok)
                self._record(caller, provider["name"], model, True,
                             latency, in_tok, out_tok, cost)
                return content
            except Exception:
                self._zhipu_working_key = None
                time.sleep(RETRY_DELAY_BASE)  # 快速路径失败后等待再进入round-robin

        # round-robin轮换
        start_idx = self._zhipu_key_index % len(keys)
        for offset in range(len(keys)):
            idx = (start_idx + offset) % len(keys)
            key = keys[idx]
            t0 = time.time()
            try:
                client = anthropic.Anthropic(api_key=key, base_url=base_url)
                kwargs = {
                    "model": model, "max_tokens": max_tokens,
                    "temperature": temperature,
                    "messages": [{"role": "user", "content": prompt}],
                }
                if system:
                    kwargs["system"] = system
                message = client.messages.create(**kwargs)
                content = message.content[0].text

                self._zhipu_working_key = key
                self._zhipu_key_index = (idx + 1) % len(keys)
                latency = int((time.time() - t0) * 1000)
                in_tok = getattr(message.usage, "input_tokens", 0) or 0
                out_tok = getattr(message.usage, "output_tokens", 0) or 0
                cost = self._estimate_cost(model, in_tok, out_tok)
                self._record(caller, provider["name"], model, True,
                             latency, in_tok, out_tok, cost)
                return content
            except Exception as e:
                latency = int((time.time() - t0) * 1000)
                self._record(caller, provider["name"], model, False,
                             latency, error=str(e)[:200])
                if offset < len(keys) - 1:
                    time.sleep(RETRY_DELAY_BASE * (offset + 1))
                continue

        logger.warning("[%s] %s所有Key均失败", caller, provider["name"])
        return None

    def _estimate_cost(self, model: str, input_tokens: int, output_tokens: int) -> float:
        """估算单次调用成本(元)"""
        # 模糊匹配model名
        for key, rates in COST_TABLE.items():
            if key in model:
                return (input_tokens * rates["input"] + output_tokens * rates["output"]) / 1000
        # 未知模型用中等估算
        return (input_tokens * 0.01 + output_tokens * 0.03) / 1000


# ── 全局单例 ──

_instance: Optional[LLMClient] = None
_instance_lock = threading.Lock()


def get_client() -> LLMClient:
    """获取全局LLMClient单例"""
    global _instance
    if _instance is None:
        with _instance_lock:
            if _instance is None:
                _instance = LLMClient()
    return _instance
