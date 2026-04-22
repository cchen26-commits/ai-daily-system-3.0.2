# AI Daily System 3.0.2

公开版 AI 日报系统搭建文档，版本基线为 `3.0.2`。

这个项目解决的是一件很具体的事：

- **自己最快速掌握当日 AI 资讯**
- **把真正值得关注的内容整理后分享给群友**

这套系统包含两条链路：

- Hermes：抓取、入库、快报、观察面同步
- OpenClaw：正式日报、飞书 docx、通知

设计原则：

- SQLite 是唯一 source of truth
- 事实层脚本化，判断层交给大模型
- 飞书多维表只是观察面，不是主库
- 标题、链接、来源、时间必须由脚本保真

## 输出效果

### Hermes 快报效果

![Hermes Digest Example](docs/assets/hermes-digest-example.png)

Hermes 的角色是提醒层：

- 快
- 轻
- 及时
- 适合个人快速掌握信息和群内轻量分享

### OpenClaw 正式日报效果

OpenClaw 的角色是正式输出层：

- [OpenClaw 正式日报示例（飞书 Docx）](https://mnc4pihca8.feishu.cn/docx/A36tdSntHoAFtGxdBetcygdDnQb?from=from_copylink)

适合：

- 正式阅读
- 分享
- 留档
- 复盘

## 工作流机制

### 1. Hermes：抓取与提醒

Hermes 负责：

- 抓取 AI 资讯
- 清洗和去重
- 写入本地 SQLite
- 在固定时间点发送快报

标准节奏：

- `07:30 / 11:30 / 15:30` 抓取
- `08:00 / 12:00 / 16:00` 发送快报

### 2. OpenClaw：正式日报输出

OpenClaw 负责：

- 从 SQLite 主库读取候选
- 基于标题做选题
- 对少量入选条目读原文
- 输出正式飞书日报

标准节奏：

- `08:30` 生成正式日报

### 3. 数据层规则

- SQLite 是唯一主库
- 飞书多维表只是观察面
- 事实层全部脚本化
- 大模型只负责判断、摘要和分析

### 4. 观察面语义

观察面不是“抓到什么就立刻显示什么”，而是：

- **这一轮实际发出了什么**

也就是说：

- SQLite 负责保存全量素材
- 飞书观察面负责展示结果视图

## 文档入口

- [文档索引](docs/INDEX.md)
- [对外说明：这套系统是怎么搭的](docs/architecture/public-build-thinking-v3.0.2.md)
- [整体搭建方案](docs/architecture/system-overview-v3.0.2.md)
- [Hermes 操作手册](docs/architecture/hermes-setup-v3.0.2.md)
- [OpenClaw 操作手册](docs/architecture/openclaw-setup-v3.0.md)
- [参考信源（公开版）](docs/sources/reference-sources-v3.0.2.md)
- [最小可运行 Scaffold](scaffold/README.md)
- [输出效果展示](docs/showcase/output-examples-v3.0.2.md)

## 快速理解

如果你只想先快速理解这套系统，建议按这个顺序看：

1. [对外说明：这套系统是怎么搭的](docs/architecture/public-build-thinking-v3.0.2.md)
2. [整体搭建方案](docs/architecture/system-overview-v3.0.2.md)
3. [文档索引](docs/INDEX.md)

## 适用对象

- 想复刻一套 AI 日报系统的工程师
- 只有单 agent 或有限 token 预算的团队
- 希望先从脚本主导、模型轻参与的架构开始落地的人
