# OpenClaw Runtime (public-release-1.0.3)

这一目录只保留 OpenClaw 公开版正式日报主链路，不包含调试脚本、历史备份、真实 `.env` 和一次性授权工具。

## 入口

- 正式日报生成：`generate-report.mjs`
- SQLite 写回：`writeback_sqlite.py`
- Windows 定时任务入口：`run-daily.cmd`

## 运行依赖

- Node.js 18+
- Python 3.10+
- `package.json`
- `config/examples/openclaw.env.example`

## 说明

- 正式日报生成依赖 SQLite 候选池。
- 标题、链接、来源等事实字段应来自数据库或脚本，不应由模型自由生成。
- 通知、封面、多维表同步均为可选能力，默认关闭。
