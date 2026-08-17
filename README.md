# Alpha Miner

Alpha Miner 把免费市场数据和行为金融假说变成可证伪的 PAPER 玩法，并用同一张玩法卡持续记录模拟动作与结果。

## 从这里开始

| 身份 | 入口文档 | 关注内容 |
|---|---|---|
| 日常用户 | [USER_GUIDE.md](USER_GUIDE.md) | 唯一命令、玩法卡解释、反馈边界 |
| PM / 负责人 | [PROJECT_STATUS.md](PROJECT_STATUS.md) | 当前证据、失败、未提交批次、下一步 |
| RD | [AGENTS.md](AGENTS.md) 与 [AGENT_ROLES.md](AGENT_ROLES.md) | 工程约束、角色权限、交付格式 |
| 架构审查 | [ARCHITECTURE.md](ARCHITECTURE.md) | 唯一闭环、数据纪律、PAPER 与准入 |

文档与实现冲突时，当前架构以 `ARCHITECTURE.md` 为准；会变化的提交、测试和数据状态只看 `PROJECT_STATUS.md`。

## USER 唯一入口

```powershell
python -m cli
```

该入口只读 SQLite 中后台预计算的玩法卡，不采集、不联网、不回测、不调用 LLM，也不写数据库。未准入玩法仍展示完整 PAPER 动作，但不构成实盘建议，真实资金仓位为 0。

## 最小架构

```text
免费前向数据
  → PIT 隔离的离线实验
  → PAPER 玩法卡与自动结算
  → python -m cli 只读展示
  → USER 反馈进入下一轮研究
```

系统坚持一引擎、一套 SQLite 数据契约、一个 USER 入口和一种玩法卡。Windows 负责前向采集与一致性发布，服务器本地 SQLite/WAL 负责慢计算；X 盘只同步代码和承载只读快照。慢任务不得阻塞 USER。

## 运行要求

- Python `>=3.11`
- 项目内 `.venv`
- `uv` 用于开发依赖与锁定环境
- SQLite 为唯一业务数据库格式

最小开发安装：

```powershell
uv sync
```

进入已创建的虚拟环境后，也可直接使用 `python -m cli`；USER 日常入口不依赖 `uv run`。

## 三角色入口

需要治理边界时，使用独立会话：

```powershell
.\scripts\agent.ps1 pm
.\scripts\agent.ps1 rd
.\scripts\agent.ps1 user
```

PM 只读治理，RD 接受有边界的实现任务，USER 只读玩法卡并反馈。角色不能在同一会话中切换；权限细节见 [AGENT_ROLES.md](AGENT_ROLES.md)。

## 开发纪律

- 所有行情与实验必须遵守 point-in-time 边界，不能用未来数据反推当日动作。
- 搜索只发生在 development；锁定测试候选冻结后只评估一次。
- PAPER 结果与实盘准入分离，不因结果难看而放宽成本、滑点、不可成交或样本规则。
- 修改前保护共享工作树，测试失败必须如实报告；发布、数据库覆盖、计划任务、commit 和 push 均需明确授权。

当前是否可发布、有哪些已知失败和未提交工作，以 [PROJECT_STATUS.md](PROJECT_STATUS.md) 为准。
