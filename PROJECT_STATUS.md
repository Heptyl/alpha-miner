# Alpha Miner 项目状态看板

> 面向 PM / 项目负责人。状态日期：2026-08-17。唯一架构源规范见 [ARCHITECTURE.md](ARCHITECTURE.md)。

## 一句话结论

一引擎、一库、一 USER 入口、一玩法卡的最小 PAPER 闭环已经完成并通过服务器离线回归；当前尚无已证明统计优势，下一步是补竞价/分钟数据并累计前向 PAPER，而不是扩玩法或宣称高胜率。

## 已验证事实

| 项目 | 当前事实 |
|---|---|
| 真实库玩法卡 | 已有 1 张 `three_to_four_reseal / 2026-08-17`，状态 `PLANNED / NOT_ADMITTED`；后台成功采集后会自动推进逐候选 PAPER 生命周期。 |
| USER 入口 | `python -m cli play` 以 SQLite `mode=ro` 读取预计算卡；展示明确模拟动作，不触发采集、回测、网络或 LLM。 |
| 远程完整回归 | native-offline Python 3.12.13：477 passed、5 skipped、7 deselected、33 warnings。 |
| 定向组合 | 玩法、报告、brain、采集 runner：97 passed、4 skipped；相关 Ruff、brain schema、PowerShell AST 和 diff-check 通过。 |
| 精简批次 | 8 个无引用诊断脚本、共 427 行已删除；远端非 purge 残留也已按精确路径删除。 |
| 计划任务 | `AlphaMiner-LimitUpHistory` 已安装并首跑成功；runner 已发布基线为 `a182e67`（Codex）。 |

## 当前证据边界

现有三进四卡以盘后日线、`open_count` 和涨停收盘价做研究/成交代理，只能证明系统首次产出了可保存、可展示、可自动结算并可被证伪的 PAPER 玩法，不能证明盘中可执行优势。

任务 4A 已确认：现有库缺少 9:25 带时间戳竞价快照、09:31 分钟 VWAP/成交和连续 1/5 分钟量价序列；60 日窗口仅有 4 个可用信号日。当前没有已证明优势，更不是高胜率玩法或实盘建议。缺少前向数据时只能报告“数据未具备”，不得用盘后封板结果反推当天买入。

## 下一步

1. Windows 连续前向采集并发布竞价和分钟数据，服务器只做离线回测；慢任务不阻塞 USER。
2. 只围绕 D-1 候选竞价接力、上涨途中分钟量价冲板、高位炸板/题材退潮卖出回避累计前向 PAPER 结果。
3. PAPER 样本和时间外证据足够后再判断优势与准入；在此之前不扩功能、不改准入门槛。

## 本轮提交

- `8fcd968` `fix(user): harden report and brief output`
- `e75c34c` `docs(architecture): focus on executable pre-limit plays`
- `67d2876` `chore(repo): remove obsolete diagnostic scripts`
- `632bfb9` `feat(play): add paper play lifecycle`

以上提交及本看板提交仅在本地创建，尚未 push。
