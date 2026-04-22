# AI Daily System 3.0.2

公开版 AI 日报系统搭建文档，版本基线为 `3.0.2`。

这套系统包含两条链路：

- Hermes：抓取、入库、快报、观察面同步
- OpenClaw：正式日报、飞书 docx、通知

设计原则：

- SQLite 是唯一 source of truth
- 事实层脚本化，判断层交给大模型
- 飞书多维表只是观察面，不是主库
- 标题、链接、来源、时间必须由脚本保真

## 文档入口

- [文档索引](docs/INDEX.md)
- [对外说明：这套系统是怎么搭的](docs/architecture/public-build-thinking-v3.0.2.md)
- [整体搭建方案](docs/architecture/system-overview-v3.0.2.md)
- [Hermes 操作手册](docs/architecture/hermes-setup-v3.0.2.md)
- [OpenClaw 操作手册](docs/architecture/openclaw-setup-v3.0.md)
- [参考信源（公开版）](docs/sources/reference-sources-v3.0.2.md)
- [最小可运行 Scaffold](scaffold/README.md)

## 快速理解

如果你只想先快速理解这套系统，建议按这个顺序看：

1. [对外说明：这套系统是怎么搭的](docs/architecture/public-build-thinking-v3.0.2.md)
2. [整体搭建方案](docs/architecture/system-overview-v3.0.2.md)
3. [文档索引](docs/INDEX.md)

## 适用对象

- 想复刻一套 AI 日报系统的工程师
- 只有单 agent 或有限 token 预算的团队
- 希望先从脚本主导、模型轻参与的架构开始落地的人
