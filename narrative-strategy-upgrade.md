# Alpha Miner — 叙事策略层升级

> **设计者**: Opus (架构设计)
> **执行者**: GLM (代码实现)
> **定位**: 在现有因子体系之上，新增一个"叙事策略层"，让 LLM 从"因子生产者"升级为"策略解释器 + 复盘师"

---

## 零、为什么要做这件事

当前系统的差异化不在数字因子（这块拼不过量化私募），而在**用行为金融理论框架解读市场叙事**。现有 4 个叙事因子（theme_lifecycle / narrative_velocity / theme_crowding / leader_clarity）太粗糙——只是把文本信息数字化了，没有真正发挥 LLM 理解语言的能力。

升级目标：**叙事做滤网（该不该做、做什么题材），数字做扳机（选哪只、何时撤）**。

---

## 一、整体架构变更

```
现有架构:
  数据采集 → 因子计算 → IC追踪 → DailyBrief(温度计+候选卡+风险预警)

升级后:
  数据采集 → 因子计算 → IC追踪 → DailyBrief
                                       ↓
                              NarrativeStrategy（新增）
                                       ↓
                              最终决策简报（LLM 生成自然语言）
```

新增模块位置: `src/narrative/` (新目录)

```
src/narrative/
├── __init__.py
├── script_engine.py      # 核心: 市场剧本引擎
├── news_classifier.py    # 新闻分类器 (利好兑现/题材发酵/业绩预期/...)
├── replay_engine.py      # 复盘引擎 (盘后 LLM 复盘)
└── prompts/
    ├── market_script.md  # 市场剧本 prompt
    ├── news_classify.md  # 新闻分类 prompt
    └── replay.md         # 复盘 prompt
```

---

## 二、模块一：新闻分类器 (`news_classifier.py`)

### 2.1 目的

当前 `narrative_velocity` 只统计新闻数量变化率，不区分新闻**类型**。同样是新闻增加，"利好兑现"和"题材发酵"对后续行情的影响完全不同。

### 2.2 新闻类别定义

```python
# src/narrative/news_classifier.py

from enum import Enum

class NewsType(str, Enum):
    """新闻对股价的影响类型。"""
    THEME_IGNITE    = "theme_ignite"     # 题材首次点燃（新概念/新政策）
    THEME_FERMENT   = "theme_ferment"    # 题材发酵中（后续跟踪报道）
    CATALYST_REAL   = "catalyst_real"    # 实质性利好（订单/业绩/中标）
    CATALYST_EXPECT = "catalyst_expect"  # 预期类利好（传闻/规划/预告）
    GOOD_REALIZE    = "good_realize"     # 利好兑现（已公告的利好落地）
    NEGATIVE        = "negative"        # 利空消息
    NOISE           = "noise"           # 无关噪音
```

### 2.3 分类方式：规则引擎 + LLM 兜底

**优先用规则引擎**（快、免费、可控），LLM 只处理规则无法判断的复杂文本。

```python
class NewsClassifier:
    """新闻分类器：规则优先，LLM 兜底。"""

    # 规则关键词表（从 a-share-sentiment 的 fin_sentiment 扩展）
    RULES = {
        NewsType.THEME_IGNITE: {
            "keywords": ["首次", "突破", "划时代", "颠覆", "新赛道", "政策出台", "国务院发布"],
            "anti_keywords": ["继续", "持续", "延续"],
        },
        NewsType.CATALYST_REAL: {
            "keywords": ["中标", "签约", "订单", "净利润增长", "营收增长", "业绩预增"],
        },
        NewsType.GOOD_REALIZE: {
            "keywords": ["正式发布", "已完成", "落地", "通过审批", "获批"],
            "context": "此前已有预期",  # 需要结合上下文
        },
        NewsType.NEGATIVE: {
            "keywords": ["处罚", "违规", "下修", "业绩预减", "退市", "ST"],
        },
    }

    def __init__(self, llm_client=None):
        self.llm_client = llm_client

    def classify(self, title: str, content: str, stock_code: str = "") -> dict:
        """
        返回:
        {
            "news_type": NewsType,
            "confidence": float,  # 0-1
            "method": "rule" | "llm",
            "reasoning": str,     # 分类理由（LLM 时有值）
        }
        """
        # Step 1: 规则匹配
        result = self._rule_classify(title, content)
        if result["confidence"] >= 0.7:
            return result

        # Step 2: LLM 分类（如果可用）
        if self.llm_client:
            return self._llm_classify(title, content, stock_code)

        # Step 3: 无 LLM 则返回规则结果（即使低置信度）
        return result

    def _rule_classify(self, title: str, content: str) -> dict:
        """基于关键词的规则分类。"""
        text = title + " " + content
        best_type = NewsType.NOISE
        best_score = 0.0

        for ntype, rule in self.RULES.items():
            score = 0.0
            keywords = rule.get("keywords", [])
            anti_keywords = rule.get("anti_keywords", [])

            for kw in keywords:
                if kw in text:
                    score += 1.0 / len(keywords)

            for akw in anti_keywords:
                if akw in text:
                    score *= 0.5

            if score > best_score:
                best_score = score
                best_type = ntype

        return {
            "news_type": best_type,
            "confidence": min(best_score, 1.0),
            "method": "rule",
            "reasoning": "",
        }

    def _llm_classify(self, title: str, content: str, stock_code: str) -> dict:
        """用 LLM 分类（仅在规则不确定时调用）。"""
        prompt_path = Path(__file__).parent / "prompts" / "news_classify.md"
        # 读取 prompt 模板，填充 title/content/stock_code
        # 调用 self.llm_client.messages.create(...)
        # 解析返回的 JSON
        # 注意: temperature=0.1, max_tokens=200
        pass  # GLM 实现
```

### 2.4 存储

在 `news` 表新增两列（ALTER TABLE，不破坏现有数据）:

```sql
ALTER TABLE news ADD COLUMN news_type TEXT DEFAULT 'noise';
ALTER TABLE news ADD COLUMN classify_confidence REAL DEFAULT 0.0;
```

### 2.5 与现有因子集成

`narrative_velocity` 升级：不再只数新闻量，而是按类型加权：

```python
# 升级后的权重
TYPE_WEIGHTS = {
    "theme_ignite": 3.0,    # 新题材点燃，权重最高
    "theme_ferment": 1.5,   # 发酵中
    "catalyst_real": 2.0,   # 实质利好
    "catalyst_expect": 1.0, # 预期类
    "good_realize": -0.5,   # 利好兑现，反而是减分
    "negative": -2.0,       # 利空
    "noise": 0.0,           # 噪音不计
}
```

---

## 三、模块二：市场剧本引擎 (`script_engine.py`)

### 3.1 目的

每天收盘后，将当日的**结构化数据**喂给 LLM，让它用 `theories.yaml` 里的理论框架写一份"今日市场剧本"。这不是因子，而是**策略层的定性判断**。

### 3.2 输入数据结构

从 `DailyBrief` 已有的结构化数据中组装：

```python
@dataclass
class MarketSnapshot:
    """喂给 LLM 的当日市场快照（结构化，非原始 DataFrame）。"""
    date: str

    # 温度计数据（来自 DailyBrief.build_thermometer）
    regime: str
    emotion_level: str
    zt_count: int
    dt_count: int
    highest_board: int
    zb_count: int

    # 涨停梯队（来自 zt_pool）
    board_ladder: list[dict]
    # 格式: [{"height": 5, "stocks": ["000001 AI龙头"], "count": 1}, ...]

    # 题材热度（来自 concept_daily）
    hot_themes: list[dict]
    # 格式: [{"theme": "AI", "zt_count": 8, "leader": "000001", "day": 3}, ...]

    # 龙虎榜摘要（来自 lhb_detail）
    lhb_summary: list[dict]
    # 格式: [{"stock": "000001", "net_buy": 5000万, "top_buyer": "东方财富拉萨"}, ...]

    # 关键新闻（已分类，来自 news + news_classifier）
    key_news: list[dict]
    # 格式: [{"stock": "000001", "title": "...", "type": "theme_ignite"}, ...]

    # 资金流概况（来自 fund_flow 聚合）
    fund_flow_summary: dict
    # 格式: {"super_large_net_total": -5亿, "large_net_total": -3亿, "direction": "流出"}
```

### 3.3 LLM Prompt: 市场剧本

创建 `src/narrative/prompts/market_script.md`:

```markdown
# 市场剧本分析师

你是一个 A 股超短线策略分析师，精通信息瀑布理论、题材生命周期、三班组手法。
你的任务是基于今日盘后数据，用这些理论框架解读市场正在演什么"剧本"。

## 你掌握的理论框架

### 信息瀑布 (BHW 1992)
- 涨停板是注意力阈值信号，触发后续跟随者放弃私有判断
- 封板稳定 → 瀑布延续 → 次日大概率高开
- 炸板 = 瀑布中断 → 反向踩踏

### 三班组识别
- 小市值 + 低换手 + 无明确题材 + 高位连板 = 游资对倒，天地板概率极高
- 超大单买 + 大单卖 = 分仓出货信号

### 题材生命周期
- 萌芽期（1-2日，1-3家涨停）：最佳介入点
- 爆发期（3-5日，涨停扩散）：龙头确认，跟风可做
- 高潮期（涨停>全市场30%，换手暴增）：顶部信号
- 衰退期（龙头炸板/开板，跟风股先跌）：离场

### 情绪 regime
- 连板潮：追龙头，看封板质量
- 题材轮动：追新不追旧，看叙事强度
- 地量：休息，等待信号
- 普涨普跌：因子失效，随大势

## 今日数据

{market_snapshot_json}

## 任务

请输出 JSON 格式:
```json
{
  "script_title": "一句话概括今天的市场剧本（10字内）",
  "script_narrative": "用 2-3 段话解释今天市场在演什么故事，哪些信号值得注意。引用具体的理论和数据。",
  "theme_verdicts": [
    {
      "theme": "题材名",
      "stage": "萌芽/爆发/高潮/衰退",
      "verdict": "明日应对建议",
      "reasoning": "判断依据"
    }
  ],
  "tomorrow_playbook": {
    "primary_strategy": "明日主策略（一句话）",
    "watch_list": ["值得关注的方向，非个股代码"],
    "avoid_list": ["应回避的方向"],
    "position_advice": "仓位建议和理由"
  },
  "risk_alerts": ["今日出现的异常信号"]
}
```

不要输出任何 JSON 以外的内容。
```

### 3.4 ScriptEngine 实现

```python
# src/narrative/script_engine.py

import json
import logging
from datetime import datetime
from dataclasses import dataclass, asdict
from pathlib import Path

from src.data.storage import Storage
from src.drift.daily_brief import DailyBrief

logger = logging.getLogger(__name__)

PROMPT_PATH = Path(__file__).parent / "prompts" / "market_script.md"


@dataclass
class MarketScript:
    """市场剧本分析结果。"""
    date: str
    script_title: str = ""
    script_narrative: str = ""
    theme_verdicts: list[dict] = None
    tomorrow_playbook: dict = None
    risk_alerts: list[str] = None
    raw_snapshot: dict = None  # 保留原始输入，便于调试

    def __post_init__(self):
        self.theme_verdicts = self.theme_verdicts or []
        self.tomorrow_playbook = self.tomorrow_playbook or {}
        self.risk_alerts = self.risk_alerts or []


class ScriptEngine:
    """市场剧本引擎：从结构化数据生成自然语言策略解读。"""

    def __init__(self, db: Storage, llm_client=None):
        self.db = db
        self.llm_client = llm_client
        self.brief = DailyBrief(db)

    def generate(self, as_of: datetime) -> MarketScript:
        """生成当日市场剧本。"""
        date_str = as_of.strftime("%Y-%m-%d")

        # 1. 组装快照
        snapshot = self._build_snapshot(as_of)

        # 2. 无 LLM 时返回纯结构化摘要
        if not self.llm_client:
            return self._fallback_script(date_str, snapshot)

        # 3. 有 LLM 时生成完整剧本
        return self._llm_generate(date_str, snapshot)

    def _build_snapshot(self, as_of: datetime) -> MarketSnapshot:
        """从数据库组装当日快照。"""
        date_str = as_of.strftime("%Y-%m-%d")

        # 温度计
        thermo = self.brief.build_thermometer(as_of)

        # 涨停梯队
        zt_df = self.db.query("zt_pool", as_of,
                              where="trade_date = ?", params=(date_str,))
        board_ladder = self._build_board_ladder(zt_df)

        # 题材热度
        concept_df = self.db.query("concept_daily", as_of,
                                   where="trade_date = ?", params=(date_str,))
        hot_themes = self._build_hot_themes(concept_df)

        # 龙虎榜摘要
        lhb_df = self.db.query("lhb_detail", as_of,
                               where="trade_date = ?", params=(date_str,))
        lhb_summary = self._build_lhb_summary(lhb_df)

        # 关键新闻（已分类）
        news_df = self.db.query("news", as_of,
                                where="publish_time LIKE ?", params=(f"{date_str}%",))
        key_news = self._build_key_news(news_df)

        # 资金流
        fund_df = self.db.query("fund_flow", as_of,
                                where="trade_date = ?", params=(date_str,))
        fund_summary = self._build_fund_summary(fund_df)

        return MarketSnapshot(
            date=date_str,
            regime=thermo.regime,
            emotion_level=thermo.emotion_level,
            zt_count=thermo.zt_count,
            dt_count=thermo.dt_count,
            highest_board=thermo.highest_board,
            zb_count=thermo.zb_count,
            board_ladder=board_ladder,
            hot_themes=hot_themes,
            lhb_summary=lhb_summary,
            key_news=key_news,
            fund_flow_summary=fund_summary,
        )

    # ── 子方法: 组装各模块数据 ─────────────────────

    def _build_board_ladder(self, zt_df) -> list[dict]:
        """从 zt_pool 构建涨停梯队。"""
        if zt_df.empty or "consecutive_zt" not in zt_df.columns:
            return []

        ladder = []
        for height, group in zt_df.groupby("consecutive_zt"):
            stocks = []
            for _, row in group.iterrows():
                code = row.get("stock_code", "")
                name = row.get("stock_name", code)
                stocks.append(f"{code} {name}")
            ladder.append({
                "height": int(height),
                "stocks": stocks[:5],  # 每层最多展示 5 只
                "count": len(group),
            })
        ladder.sort(key=lambda x: x["height"], reverse=True)
        return ladder

    def _build_hot_themes(self, concept_df) -> list[dict]:
        """从 concept_daily 构建题材热度。"""
        if concept_df.empty:
            return []

        themes = []
        for _, row in concept_df.iterrows():
            zt_count = int(row.get("zt_count", 0))
            if zt_count < 2:  # 过滤低热度
                continue
            themes.append({
                "theme": row.get("concept_name", ""),
                "zt_count": zt_count,
                "leader": row.get("leader_code", ""),
                "leader_consecutive": int(row.get("leader_consecutive", 0)),
            })
        themes.sort(key=lambda x: x["zt_count"], reverse=True)
        return themes[:10]  # 最多 10 个

    def _build_lhb_summary(self, lhb_df) -> list[dict]:
        """从 lhb_detail 构建龙虎榜摘要。"""
        if lhb_df.empty:
            return []
        # 按 stock_code 聚合净买入
        # GLM 实现: 聚合 buy_amount - sell_amount, 取 top 5
        return []

    def _build_key_news(self, news_df) -> list[dict]:
        """从 news 表构建关键新闻列表（已分类的）。"""
        if news_df.empty:
            return []
        # 过滤掉 noise 类型，保留 theme_ignite / catalyst_real 等
        # 按 classify_confidence 降序，取 top 10
        key = []
        for _, row in news_df.iterrows():
            ntype = row.get("news_type", "noise")
            if ntype == "noise":
                continue
            key.append({
                "stock": row.get("stock_code", ""),
                "title": row.get("title", ""),
                "type": ntype,
                "sentiment": float(row.get("sentiment_score", 0.5)),
            })
        return key[:10]

    def _build_fund_summary(self, fund_df) -> dict:
        """资金流聚合。"""
        if fund_df.empty:
            return {"super_large_net_total": 0, "large_net_total": 0, "direction": "未知"}

        sln = fund_df["super_large_net"].sum() if "super_large_net" in fund_df.columns else 0
        ln = fund_df["large_net"].sum() if "large_net" in fund_df.columns else 0
        direction = "流入" if (sln + ln) > 0 else "流出"
        return {
            "super_large_net_total": float(sln),
            "large_net_total": float(ln),
            "direction": direction,
        }

    # ── LLM 生成 ──────────────────────────────────

    def _llm_generate(self, date_str: str, snapshot: MarketSnapshot) -> MarketScript:
        """用 LLM 生成市场剧本。"""
        prompt_template = PROMPT_PATH.read_text() if PROMPT_PATH.exists() else ""
        snapshot_json = json.dumps(asdict(snapshot), ensure_ascii=False, indent=2)
        prompt = prompt_template.replace("{market_snapshot_json}", snapshot_json)

        try:
            response = self.llm_client.messages.create(
                model="glm-4-plus",  # GLM 模型
                max_tokens=1500,
                temperature=0.3,     # 偏确定性，但允许一定表达自由
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text

            # 清理 JSON
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            elif "```" in text:
                text = text.split("```")[1].split("```")[0]

            data = json.loads(text.strip())
            return MarketScript(
                date=date_str,
                script_title=data.get("script_title", ""),
                script_narrative=data.get("script_narrative", ""),
                theme_verdicts=data.get("theme_verdicts", []),
                tomorrow_playbook=data.get("tomorrow_playbook", {}),
                risk_alerts=data.get("risk_alerts", []),
                raw_snapshot=asdict(snapshot),
            )
        except Exception as e:
            logger.error("剧本生成失败: %s", e)
            return self._fallback_script(date_str, snapshot)

    def _fallback_script(self, date_str: str, snapshot: MarketSnapshot) -> MarketScript:
        """无 LLM 时的纯规则回退。"""
        title = f"{snapshot.regime} | 涨停{snapshot.zt_count} 跌停{snapshot.dt_count}"
        narrative = (
            f"今日 regime={snapshot.regime}，涨停{snapshot.zt_count}家，"
            f"跌停{snapshot.dt_count}家，最高板{snapshot.highest_board}连板，"
            f"炸板{snapshot.zb_count}家。情绪级别: {snapshot.emotion_level}。"
        )
        return MarketScript(
            date=date_str,
            script_title=title,
            script_narrative=narrative,
            raw_snapshot=asdict(snapshot),
        )
```

---

## 四、模块三：复盘引擎 (`replay_engine.py`)

### 4.1 目的

让 LLM 对比"昨日预测 vs 今日实际"，生成复盘，闭环学习。

### 4.2 实现

```python
# src/narrative/replay_engine.py

class ReplayEngine:
    """复盘引擎：对比昨日剧本 vs 今日实际。"""

    def __init__(self, db: Storage, llm_client=None):
        self.db = db
        self.llm_client = llm_client

    def replay(self, today: datetime) -> dict:
        """
        1. 读取昨日的 MarketScript (存在 market_scripts 表)
        2. 读取今日的实际数据
        3. 让 LLM 对比，输出:
           - 昨日预测哪些对了
           - 哪些错了、为什么
           - 对理论框架的修正建议
        """
        # 读昨日剧本
        yesterday_script = self._load_yesterday_script(today)
        if not yesterday_script:
            return {"status": "no_previous_script"}

        # 读今日实际
        today_actual = self._build_today_actual(today)

        if not self.llm_client:
            return {"status": "no_llm", "yesterday": yesterday_script}

        # LLM 复盘
        prompt = self._build_replay_prompt(yesterday_script, today_actual)
        try:
            response = self.llm_client.messages.create(
                model="glm-4-plus",
                max_tokens=1000,
                temperature=0.3,
                messages=[{"role": "user", "content": prompt}],
            )
            text = response.content[0].text
            if "```json" in text:
                text = text.split("```json")[1].split("```")[0]
            return json.loads(text.strip())
        except Exception as e:
            logger.error("复盘失败: %s", e)
            return {"status": "error", "detail": str(e)}

    def _load_yesterday_script(self, today: datetime) -> dict | None:
        """从 market_scripts 表读取昨日剧本。"""
        # GLM 实现: 查询 market_scripts 表，取 today - 1 交易日的记录
        pass

    def _build_today_actual(self, today: datetime) -> dict:
        """组装今日实际数据用于对比。"""
        # 复用 ScriptEngine._build_snapshot()
        pass

    def _build_replay_prompt(self, yesterday: dict, today: dict) -> str:
        """构建复盘 prompt。"""
        return f"""你是一个超短线复盘分析师。

昨日策略预判:
{json.dumps(yesterday, ensure_ascii=False, indent=2)}

今日实际情况:
{json.dumps(today, ensure_ascii=False, indent=2)}

请输出 JSON:
{{
  "accuracy_score": 0-10,
  "correct_calls": ["昨日判断正确的点"],
  "wrong_calls": [
    {{"prediction": "昨日说了什么", "actual": "实际发生了什么", "why_wrong": "原因分析"}}
  ],
  "theory_feedback": "对理论框架的修正建议（如果有的话）",
  "learning": "今天最值得记住的一条经验"
}}

不要输出 JSON 以外的内容。"""
```

---

## 五、数据库变更

### 5.1 新增表: `market_scripts`

在 `src/data/schema.sql` 末尾追加:

```sql
CREATE TABLE IF NOT EXISTS market_scripts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    trade_date TEXT NOT NULL,
    script_title TEXT,
    script_narrative TEXT,
    theme_verdicts TEXT,        -- JSON
    tomorrow_playbook TEXT,     -- JSON
    risk_alerts TEXT,           -- JSON
    raw_snapshot TEXT,          -- JSON (完整输入数据，便于复盘)
    replay_result TEXT,         -- JSON (次日复盘结果)
    snapshot_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(trade_date)
);
```

### 5.2 修改 `news` 表

```sql
ALTER TABLE news ADD COLUMN news_type TEXT DEFAULT 'noise';
ALTER TABLE news ADD COLUMN classify_confidence REAL DEFAULT 0.0;
```

---

## 六、CLI 集成

### 6.1 在 `cli/report.py` 中新增子命令

```python
# 在现有 report 命令组中增加:

@report.command("script")
@click.option("--date", default=None, help="日期 YYYY-MM-DD")
def cmd_script(date):
    """生成市场剧本（叙事策略层）。"""
    # 1. 构建 LLM client (复用 cli/mine.py 的 _build_llm_client)
    # 2. 实例化 ScriptEngine
    # 3. 调用 engine.generate(as_of)
    # 4. 存入 market_scripts 表
    # 5. rich 美化输出
    pass


@report.command("replay")
@click.option("--date", default=None, help="日期 YYYY-MM-DD")
def cmd_replay(date):
    """复盘：对比昨日剧本 vs 今日实际。"""
    # 1. 构建 LLM client
    # 2. 实例化 ReplayEngine
    # 3. 调用 engine.replay(today)
    # 4. 更新 market_scripts 表的 replay_result 字段
    # 5. rich 美化输出
    pass
```

### 6.2 在 `scripts/daily_run.sh` 中追加步骤

```bash
# 现有流程:
# python -m cli collect --today
# python -m cli backtest --compute-today
# python -m cli report --brief

# 新增（在 report --brief 之后）:
python -m cli report script          # 生成今日剧本
python -m cli report replay          # 复盘昨日预测
```

---

## 七、升级现有 `narrative_velocity` 因子

### 7.1 目标

在 `src/factors/narrative/narrative_velocity.py` 中，利用新闻分类结果做加权。

### 7.2 改动

```python
# src/factors/narrative/narrative_velocity.py

# 新增: 新闻类型权重
NEWS_TYPE_WEIGHTS = {
    "theme_ignite": 3.0,
    "theme_ferment": 1.5,
    "catalyst_real": 2.0,
    "catalyst_expect": 1.0,
    "good_realize": -0.5,
    "negative": -2.0,
    "noise": 0.0,
}

class NarrativeVelocityFactor(BaseFactor):
    def compute(self, universe, as_of, db):
        # ... 现有逻辑保持不变 ...

        # 新增: 如果 news 表有 news_type 列，用加权计算
        if "news_type" in news_today.columns:
            today_counts = {}
            for code in universe:
                stock_news = news_today[news_today["stock_code"] == code]
                weighted_sum = sum(
                    NEWS_TYPE_WEIGHTS.get(row.get("news_type", "noise"), 0)
                    for _, row in stock_news.iterrows()
                )
                today_counts[code] = weighted_sum
        # else: 退回到原有的纯数量统计
```

---

## 八、执行步骤（给 GLM 的 checklist）

严格按以下顺序执行，每步完成后运行测试确认。

### Step 1: 数据库变更
- [ ] 在 `src/data/schema.sql` 追加 `market_scripts` 表
- [ ] 在 `src/data/storage.py` 的 `init_db()` 中执行 `news` 表的 ALTER TABLE（加两列）
- [ ] 用 `try/except` 包裹 ALTER TABLE（列已存在时不报错）
- [ ] 测试: `pytest tests/test_storage.py -v`

### Step 2: 新闻分类器
- [ ] 创建 `src/narrative/__init__.py`
- [ ] 创建 `src/narrative/news_classifier.py`（完整实现规则引擎部分，LLM 部分留 stub）
- [ ] 创建 `src/narrative/prompts/news_classify.md`
- [ ] 写测试: `tests/test_news_classifier.py` — 用固定标题验证分类结果
- [ ] 测试: `pytest tests/test_news_classifier.py -v`

### Step 3: 在采集流程中集成新闻分类
- [ ] 修改 `src/data/sources/akshare_news.py`，在 `fetch()` 末尾调用 `NewsClassifier.classify()` 填充 `news_type` 和 `classify_confidence`
- [ ] 不传 `llm_client`（采集时只用规则引擎，不调 LLM）
- [ ] 测试: `pytest tests/ -v -m "not live"`

### Step 4: 市场剧本引擎
- [ ] 创建 `src/narrative/script_engine.py`（完整实现 `_build_snapshot` 的所有子方法）
- [ ] 创建 `src/narrative/prompts/market_script.md`（从上面第三节复制）
- [ ] 特别注意: `_build_lhb_summary()` 需要完整实现聚合逻辑
- [ ] 写测试: `tests/test_script_engine.py` — mock 数据测试 `_build_snapshot` 和 `_fallback_script`
- [ ] 测试: `pytest tests/test_script_engine.py -v`

### Step 5: 复盘引擎
- [ ] 创建 `src/narrative/replay_engine.py`
- [ ] 创建 `src/narrative/prompts/replay.md`
- [ ] 实现 `_load_yesterday_script()` 和 `_build_today_actual()`
- [ ] 写测试: `tests/test_replay_engine.py`
- [ ] 测试: `pytest tests/test_replay_engine.py -v`

### Step 6: CLI 集成
- [ ] 在 `cli/report.py` 新增 `script` 和 `replay` 子命令
- [ ] 复用 `cli/mine.py` 的 `_build_llm_client()` (提取到公共模块 `cli/utils.py`)
- [ ] `rich` 格式化剧本输出（Panel + Markdown）
- [ ] 更新 `scripts/daily_run.sh`

### Step 7: 升级 narrative_velocity
- [ ] 修改 `src/factors/narrative/narrative_velocity.py` 支持加权模式
- [ ] 保持向后兼容（无 `news_type` 列时退回纯数量统计）
- [ ] 测试: `pytest tests/test_narrative_factors.py -v`

### Step 8: 全量验证
- [ ] `pytest tests/ -v -m "not live"` 全部通过
- [ ] 更新 `README.md` 的架构图和命令列表
- [ ] 更新 `CLAUDE.md` 新增模块说明
- [ ] 更新 `BUILD_LOG.md` 记录本次变更

---

## 九、关键约束（GLM 必须遵守）

1. **不改现有接口**: `DailyBrief` 的 `build_thermometer` / `build_candidates` / `build_alerts` 保持不动。新模块在其之上运行。
2. **LLM 可选**: 所有新模块都必须有 `llm_client=None` 时的 fallback 路径。没有 LLM 时系统照常运行，只是没有自然语言剧本。
3. **时间隔离**: 新模块中任何数据查询都必须通过 `db.query(table, as_of=...)`, 不能直接 SQL。
4. **测试覆盖**: 每个新模块至少 5 个测试用例，使用 mock 数据，不依赖网络。
5. **日志规范**: 用 `logging.getLogger(__name__)`，不要 print。CLI 输出用 `rich`。
6. **LLM 调用**: 使用 `cli/mine.py` 已有的 `_build_llm_client()` 模式，API 走 Z.AI 的 Anthropic 兼容端点。模型名用 `glm-4-plus`。`temperature=0.3`（剧本/复盘用），`temperature=0.1`（新闻分类用）。
7. **JSON 安全**: LLM 返回的 JSON 必须用 `try/except` 包裹 `json.loads()`，失败时 fallback 到规则结果。

---

## 十、成功标准

- [ ] `python -m cli report script --date 2024-06-15` 能输出结构化剧本（即使没有 LLM，也能输出规则版）
- [ ] `python -m cli report replay --date 2024-06-16` 能对比昨日剧本与今日实际
- [ ] 新闻采集后 `news` 表的 `news_type` 列有值
- [ ] `narrative_velocity` 因子在有分类数据时使用加权，无分类数据时退回原逻辑
- [ ] 全部现有 69 个测试 + 新增测试 全部通过
- [ ] 无未来数据泄露（validate_no_future 检查通过）
