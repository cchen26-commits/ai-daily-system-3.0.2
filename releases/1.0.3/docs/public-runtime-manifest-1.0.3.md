# public-release-1.0.3 manifest

## 目标

这一目录是双 agent 日报系统的公开归集版：

- `1.0.3` 表示当前双 agent 架构的公开归档版
- `2.0` 预留给未来单 agent 收敛版

## 包含内容

### runtime/hermes

- 抓取入库
- 快报生成
- 观察面同步
- SQLite 配置与数据库原语

### runtime/openclaw

- 正式日报生成
- SQLite 写回
- Windows 定时任务入口
- Node / Python 最小运行依赖

### config/examples

- Hermes 环境变量模板
- Hermes cron 模板
- OpenClaw 环境变量模板

### docs

- 清洗计划
- 公开版 manifest

## 明确排除

以下内容不进入公开版 runtime：

- 真实 `.env`
- token / app secret / open_id / table_id
- 本地日志与输出文件
- SQLite 数据库文件
- `test_* / debug_* / check_* / *.bak`
- 一次性授权脚本
- 仅用于事故排查的临时脚本
- 本地绝对路径绑定

## 当前边界

这不是原始工作目录快照，而是经过清洗后的公开版运行时。

### 保留原则

- 能代表主链路的保留
- 仅用于当前个人环境排障的移除
- 所有私有配置改为 example 模板

### 运行语义

- Hermes：以 SQLite 为主库，观察面为后置同步
- OpenClaw：以 SQLite 候选池为输入，输出正式日报和通知
