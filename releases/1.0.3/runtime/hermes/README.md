# Hermes Runtime (public-release-1.0.3)

这一目录只保留 Hermes 公开版主链路运行时，不包含日志、数据库、调试产物和私有配置。

## 入口

- 抓取入库：`scripts/fetch_and_write.py`
- 快报生成：`scripts/generate_short_report.py`
- 观察面同步：`scripts/sync_to_bitable.py`
- SQLite 原语：`runtime/db.py`
- 配置入口：`runtime/config.py`

## 运行依赖

- Python 3.10+
- SQLite
- `config/examples/hermes_env.sh.example`
- `config/examples/hermes.crontab.example`

## 说明

- 事实层以 SQLite 为主库。
- 观察面同步为后置动作，不在抓取阶段执行。
- 该目录是公开版收敛结果，不等同于原始工作目录。
