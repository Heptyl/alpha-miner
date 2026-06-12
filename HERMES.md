<!-- superpowers-zh:begin (do not edit between these markers) -->
# Alpha Miner 技能体系

本系统整合了5大技能来源，按场景自动匹配。收到任务时优先检查是否有匹配skill。

## 核心工作流 (superpowers-zh)

设计先于编码 → 测试先于实现 → 验证先于完成。

| 触发场景 | Skill | 说明 |
|----------|-------|------|
| 收到任何新功能/需求 | brainstorming | 先想清楚再做，探索意图和设计 |
| 多步骤任务/实现方案确定后 | writing-plans | 拆成可执行的bite-sized任务 |
| 按计划逐步实施 | executing-plans | 每步验证，检查点审查 |
| 写代码前 | test-driven-development | RED-GREEN-REFACTOR |
| 遇到bug/异常 | systematic-debugging | 4阶段：定位→分析→假设→修复 |
| 任务完成前 | verification-before-completion | 证据先行，跑验证命令 |
| 代码质量检查 | requesting-code-review | 派agent审查 |
| 收到review反馈 | receiving-code-review | 技术严谨处理，不敷衍 |
| 多个独立任务并行 | dispatching-parallel-agents | 多agent并发 |
| 计划执行阶段 | subagent-driven-development | 每任务一agent，两轮审查 |
| 开发分支收尾 | finishing-a-development-branch | 合并/PR/清理 |
| Git隔离开发 | using-git-worktrees | 隔离式worktree |
| 创建新skill | writing-skills | skill创建方法论 |
| YAML多角色编排 | workflow-runner | 多角色YAML工作流 |
| MCP服务器构建 | mcp-builder | 系统化构建MCP工具 |
| 如何使用本体系 | using-superpowers | 元技能：调用优先级 |
| 中文review | chinese-code-review | 国内团队review话术 |
| 中文commit | chinese-commit-conventions | Conventional Commits中文适配 |
| 中文文档 | chinese-documentation | 中文排版+术语规则 |
| 国内Git平台 | chinese-git-workflow | Gitee/Coding/极狐配置 |

## 深度推理 (super-hermes prism)

对重要问题做多角度深度分析。

| 触发场景 | Skill | 说明 |
|----------|-------|------|
| 分析任何代码/设计/决策 | prism-scan | 单pass动态认知透镜分析 |
| 重要代码/架构必须深度审查 | prism-full | 多pass + 对抗性自我纠错 |
| 需要"为什么这样做"的深层理解 | prism-3way | WHERE/WHEN/WHY三正交分析+合成 |
| 不确定该从哪个角度分析 | prism-discover | 发现所有可能的分析维度 |
| 分析完成后怀疑遗漏了什么 | prism-reflect | 分析自己的分析盲区 |

## 多Agent编排 (oh-my-hermes)

复杂任务用多角色共识保证质量。

| 触发场景 | Skill | 说明 |
|----------|-------|------|
| 端到端自动化(从想法到代码) | omh-autopilot | interview→plan→execute→QA |
| 重要方案需多方共识 | omh-ralplan | 计划者+架构师+评审者三方≤3轮 |
| 执行计划(每步验证) | omh-ralph | 1任务/调用，铁律验证 |
| 深度网络调研 | omh-deep-research | 多agent并行搜索+合成+引用验证 |
| 需求不清晰 | omh-deep-interview | 苏格拉底式追问澄清 |
| 任务积压整理 | omh-triage | 多角色共识排序 |

*(driver后缀的是编排器的编排器，正常不需要手动调用)*

## 知识管理 (gbrain)

从Garry Tan(YC CEO)的知识系统精简而来。

| 触发场景 | Skill | 说明 |
|----------|-------|------|
| 信号/模式检测 | gbrain-signal-detector | 发现非显而易见的模式 |
| 数据驱动研究 | gbrain-data-research | 系统化数据研究方法 |
| 概念合成/跨领域连接 | gbrain-concept-synthesis | 发现不同概念间的隐含关系 |
| 查询已有知识 | gbrain-query | 结构化知识检索 |
| 自动简报生成 | gbrain-briefing | 生成结构化简报 |
| 战略性阅读 | gbrain-strategic-reading | 深度阅读方法论 |
| 项目架构分析 | gbrain-repo-architecture | 理解代码库架构 |
| 知识摄入 | gbrain-idea-ingest | 结构化想法/灵感录入 |
| 学术验证 | gbrain-academic-verify | 论文/学术内容验证 |
| 书籍知识镜像 | gbrain-book-mirror | 书籍要点提取 |
| 深度搜索(需Perplexity) | gbrain-perplexity-research | Perplexity深度搜索 |
| 跨模态交叉验证 | gbrain-cross-modal-review | 多源信息交叉验证 |
| 系统维护 | gbrain-maintain | 知识库维护 |

## 自我进化 (hermes-dojo)

| 触发场景 | Skill | 说明 |
|----------|-------|------|
| 性能分析/弱点识别 | hermes-dojo | 监控表现，识别弱点，自动迭代改进 |

## 项目专属

| 触发场景 | Skill | 位置 |
|----------|-------|------|
| A股综合分析 | a-share-sentiment | 全局 |
| alpha-miner操作指南 | alpha-miner | projects/ |
| 统一工作流 | alpha-miner-workflow | projects/ |
| 双重身份思考协议 | dual-identity-protocol | projects/ |
| 会话恢复 | session-restore | devops/ |
| 数据验证铁律 | validate-before-conclude | software-development/ |
| 调研先行 | research-first | software-development/ |
| 实验验证想法 | spike | software-development/ |
| Hermes技能编写 | hermes-agent-skill-authoring | software-development/ |
| Python调试 | python-debugpy | software-development/ |

## 工具映射

技能中引用的 Claude Code 工具名称对应 Hermes Agent 的等价工具：
- `Read` → `read_file`
- `Write` → `write_file`
- `Edit` → `patch`
- `Bash` → `terminal`
- `Grep` / `Glob` → `search_files`
- `Skill` → `skill_view`
- `Task`（子智能体） → `delegate_task`
- `WebSearch` → `web_search`
- `WebFetch` → `web_extract`
- `TodoWrite` → `todo`

## 决策优先级

当任务匹配多个skill时，按以下优先级：
1. **项目专属** (dual-identity, validate-before-conclude, alpha-miner)
2. **核心工作流** (superpowers-zh: brainstorming → writing-plans → TDD → debugging → verification)
3. **深度推理** (prism: 对重要决策/架构做深度分析)
4. **多Agent编排** (omh: 复杂方案需共识时用ralplan)
5. **知识管理** (gbrain: 研究调研时用)
6. **自我进化** (dojo: 定期回顾改进)
<!-- superpowers-zh:end -->
