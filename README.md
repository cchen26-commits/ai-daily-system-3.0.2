# AI Daily System

<!-- Badges -->
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python Version](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/)
[![Last Commit](https://img.shields.io/github/last-commit/captainchn/ai-daily-system)](https://github.com/captainchn/ai-daily-system/commits/main)
[![Stars](https://img.shields.io/github/stars/captainchn/ai-daily-system?style=social)](https://github.com/captainchn/ai-daily-system/stargazers)

> 一个面向 **个人快速掌握 AI 资讯** 与 **微信群高质量分享** 的日报系统。

## 目录

- [项目概述](#项目概述)
- [核心特性](#核心特性)
- [输出效果](#输出效果)
- [工作流机制](#工作流机制)
- [架构原则](#架构原则)
- [发布版运行时](#发布版运行时)
- [快速开始](#快速开始)
- [适用对象](#适用对象)
- [文档索引](#文档索引)

---

## 项目概述

AI Daily System 解决的是信息过载时代的两个核心问题：

1. **个人**：每天不需要刷十几个来源，快速掌握最值得关注的 AI 动态
2. **分享**：把真正值得看的内容，高效整理后分享给群友

系统采用"先抓全、再筛重点、再输出两层结果"的工作流——不是"媒体级内容工厂"，而是一个更务实的信息处理工作流。

### 两层输出

| 层 | 名称 | 定位 |
|----|------|------|
| 提醒层 | **Hermes** | 快报 / 提醒，快速掌握重点 |
| 输出层 | **OpenClaw** | 正式日报 / 留档，适合分享和复盘 |

---

## 核心特性

| 特性 | 说明 |
|------|------|
| **脚本优先** | 标题、链接、来源、时间等事实字段全部脚本化保真 |
| **SQLite 主库** | 本地 SQLite 是唯一 source of truth，不依赖第三方平台 |
| **LLM 仅做判断** | 大模型只负责判断、摘要、分析，不生成事实 |
| **双层输出** | Hermes 负责快报，OpenClaw 负责正式日报 |
| **飞书友好** | 适合飞书私聊、群分享、文档沉淀 |
| **可扩展** | 换信源后可适配其他行业情报系统 |

---

## 输出效果

### Hermes 快报

![Hermes Digest Example](docs/assets/hermes-digest-example.png)

适合：快速掌握一轮重点、个人阅读、群内轻量分享

### OpenClaw 正式日报

![OpenClaw Doc Example](docs/assets/openclaw-doc-example.png)

[查看 OpenClaw 正式日报示例（飞书 Docx）](https://mnc4pihca8.feishu.cn/docx/A36tdSntHoAFtGxdBetcygdDnQb?from=from_copylink)

适合：正式阅读、群分享、留档、复盘

---

## 工作流机制

### Hermes

**职责**：抓取 → 清洗去重 → 入库 → 生成快报 → 发送同步

**标准节奏**：
- `07:30 / 11:30 / 15:30` 抓取
- `08:00 / 12:00 / 16:00` 发送快报

### OpenClaw

**职责**：从 SQLite 读取 → 正式选题 → 读原文 → 生成日报

**标准节奏**：
- `08:30` 生成正式日报

---

## 架构原则

### 1. SQLite 是唯一主库

- SQLite 保存全量事实
- 飞书多维表不是主库，仅作观察面
- 所有链路都围绕 SQLite 运转

### 2. 事实层脚本化

脚本负责：标题 / 链接 / 来源 / 时间 / 去重 / 候选池构建 / 状态写回

### 3. 判断层交给大模型

大模型仅负责：哪些值得关注 / 摘要 / 分析

### 4. 观察面是结果视图

飞书展示的是"这一轮实际处理并发出的结果"，不是原始素材池。

---

## 发布版运行时

仓库现在同时包含两类内容：

1. **公开文档与 scaffold**
2. **双 agent 架构的公开归档运行时（1.0.3）**

`1.0.3` 目录位置：

- [releases/1.0.3](releases/1.0.3/README.md)

内容包括：

- `runtime/hermes/`
- `runtime/openclaw/`
- `config/examples/`
- 发布说明与可发布清单

这部分不是原始工作目录快照，而是经过清洗后的公开版运行时。

---

## 快速开始

### 方式一：直接跑最小版本

```bash
cd scaffold
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python3 scripts/init_db.py
python3 scripts/fetch_rss.py
python3 scripts/build_digest.py
python3 scripts/render_cards.py
```

输出：`output/digest_*.md` 和 `output/cards_*.html`

### 方式二：理解完整设计

建议阅读顺序：

1. [搭建思路（对外说明）](docs/architecture/public-build-thinking-v3.0.2.md)
2. [整体搭建方案](docs/architecture/system-overview-v3.0.2.md)
3. [Hermes 操作手册](docs/architecture/hermes-setup-v3.0.2.md)
4. [OpenClaw 操作手册](docs/architecture/openclaw-setup-v3.0.md)

---

## 适用对象

- 想复刻 AI 日报系统的工程师
- 需要微信群/飞书分享的内容团队
- 只有有限 token 预算、希望"脚本多做、模型少做"的团队
- 想把同样思路迁移到其他行业情报系统的人

---

## 文档索引

| 分类 | 文档 |
|------|------|
| 架构 | [搭建思路](docs/architecture/public-build-thinking-v3.0.2.md) · [整体方案](docs/architecture/system-overview-v3.0.2.md) |
| 操作 | [Hermes 手册](docs/architecture/hermes-setup-v3.0.2.md) · [OpenClaw 手册](docs/architecture/openclaw-setup-v3.0.md) |
| 发布版 | [1.0.3 运行时归档](releases/1.0.3/README.md) |
| 原则 | [脚本优先](docs/principles/script-first.md) · [Skill 收敛](docs/principles/skill-convergence.md) |
| 参考 | [信源列表](docs/sources/reference-sources-v3.0.2.md) · [效果展示](docs/showcase/output-examples-v3.0.2.md) |
| 其他 | [最小可运行 Scaffold](scaffold/README.md) · [更新日志](CHANGELOG.md) |

---

## 致谢

如果这个项目对你有帮助，欢迎点一个 **Star**。

如果你想参与贡献，请阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

---

*如果你觉得这套思路可以迁移到其他垂直行业，欢迎交流。*
