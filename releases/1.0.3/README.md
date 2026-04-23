# Release 1.0.3

这是当前双 agent 日报系统的公开归档版。

## 定位

- `1.0.3`：当前双 agent 架构的公开归集版
- `2.0`：预留给未来单 agent 收敛版

## 包含内容

- `runtime/hermes/`：抓取、入库、快报、观察面同步
- `runtime/openclaw/`：正式日报生成、SQLite 写回、定时任务入口
- `config/examples/`：公开模板配置
- `docs/`：发布说明和可发布清单

## 不包含

- 原始导出
- 真实 `.env`
- token、secret、open_id、table_id
- 调试、测试、备份、一次性授权脚本

## 阅读顺序

1. [发布 manifest](docs/public-runtime-manifest-1.0.3.md)
2. [可发布文件清单](docs/publishable-files-1.0.3.md)
3. `runtime/hermes/README.md`
4. `runtime/openclaw/README.md`
