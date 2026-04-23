# publishable files (1.0.3)

这个目录是可直接同步到公开仓库的发布版，不包含原始导出和敏感信息留档。

## 发布目录

- `publish-1.0.3/`

## 文件清单

### 根目录

- `.gitignore`

### config/examples

- `hermes.crontab.example`
- `hermes_env.sh.example`
- `openclaw.env.example`

### docs

- `public-runtime-manifest-1.0.3.md`
- `publishable-files-1.0.3.md`

### runtime/hermes

- `README.md`
- `runtime/config.py`
- `runtime/db.py`
- `scripts/check_fetch.py`
- `scripts/check_report.py`
- `scripts/fetch_and_write.py`
- `scripts/fetch_news.py`
- `scripts/generate_short_report.py`
- `scripts/sync_to_bitable.py`
- `scripts/textrank_summary.py`

### runtime/openclaw

- `README.md`
- `generate-report.mjs`
- `package.json`
- `package-lock.json`
- `run-daily.cmd`
- `writeback_sqlite.py`

## 明确排除

以下内容不在发布版中：

- `raw/` 原始导出目录
- 真实 `.env`
- token / secret / open_id / table_id
- 日志和数据库文件
- 调试、测试、备份脚本
- 一次性授权脚本
- 仅用于当前个人环境的故障处理文件
