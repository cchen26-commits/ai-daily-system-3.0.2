# Minimal Runnable Scaffold

这个目录提供一个最小可运行版本，用来验证这套系统的核心思路：

- 脚本抓取事实字段
- SQLite 作为主库
- 先抓取，再筛选，再输出
- 不依赖大模型也能先跑通基本链路

## 目录

- `config/sources.example.yaml`：参考信源配置
- `scripts/init_db.py`：初始化 SQLite
- `scripts/fetch_rss.py`：抓取 RSS 并入库
- `scripts/build_digest.py`：生成 Markdown 快报
- `scripts/render_cards.py`：生成简单 HTML 信息卡片

## 快速开始

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

输出目录：

- `output/digest_*.md`
- `output/cards_*.html`

## 设计边界

这个 scaffold 只解决三件事：

1. 抓取真实标题/链接/来源/时间
2. 入 SQLite
3. 生成一个可以查看和分享的最小结果

它不试图在这个层面解决：

- 大模型摘要
- 飞书文档输出
- 观察面同步
- 复杂打分
- 多 agent 协作

这些能力看仓库里的正式架构文档。
