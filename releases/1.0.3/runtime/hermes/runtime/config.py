#!/usr/bin/env python3
"""
Hermes 配置层
所有运行时配置集中在此，不散落硬编码。
敏感信息从环境变量读取，非敏感参数直接写在这里。
"""
import os
import sys
from datetime import timezone, timedelta
from pathlib import Path

# ── 时区 ──────────────────────────────────────────────
BJT = timezone(timedelta(hours=8))

# ── 路径 ──────────────────────────────────────────────
DEFAULT_BASE   = Path(__file__).resolve().parents[1]
HERMES_BASE    = os.environ.get("HERMES_BASE", str(DEFAULT_BASE))
HERMES_SCRIPTS = os.path.join(HERMES_BASE, "scripts")
HERMES_RUNTIME = os.path.join(HERMES_BASE, "runtime")
HERMES_LOGS    = os.environ.get("HERMES_LOGS",   os.path.join(HERMES_BASE, "logs"))
HERMES_DB      = os.environ.get("HERMES_DB",      os.path.join(HERMES_RUNTIME, "shared_materials.db"))

# ── SQLite ─────────────────────────────────────────────
SCHEMA_VERSION = 1  # materials 表当前 schema 版本

# ── 飞书多维表（观察面，可选同步）─────────────────────
# BASE_TOKEN 和 TABLE_ID 从环境变量读取，无默认值
# 向后兼容：优先 HERMES_BITABLE_BASE_TOKEN（旧 key HERMES_BITABLE_TOKEN 仍可读）
_tmp = os.environ.get("HERMES_BITABLE_BASE_TOKEN", "") or \
       os.environ.get("HERMES_BITABLE_TOKEN", "")
BITABLE_BASE_TOKEN = _tmp
if os.environ.get("HERMES_BITABLE_TOKEN") and not os.environ.get("HERMES_BITABLE_BASE_TOKEN"):
    print("[config] ⚠️  HERMES_BITABLE_TOKEN 为旧 key，建议改配 HERMES_BITABLE_BASE_TOKEN（语义更清晰）", file=sys.stderr)
BITABLE_TABLE_ID   = os.environ.get("HERMES_BITABLE_TABLE", "")

# 多维表同步开关（True=同步，False=仅观察）
BITABLE_SYNC_ENABLED = os.environ.get("HERMES_BITABLE_SYNC", "true").lower() == "true"

# ── 快报发送 ──────────────────────────────────────────
HERMES_SEND_ENABLED = os.environ.get("HERMES_SEND_ENABLED", "true").lower() == "true"

# ── 抓取参数 ───────────────────────────────────────────
FETCH_CANDIDATES_HOURS = 4   # SQLite 候选时间窗（小时）
FETCH_CANDIDATES_LIMIT = 200  # SQLite 候选上限

# ── 快报参数 ───────────────────────────────────────────
DIGEST_CANDIDATES_HOURS = 4   # 快报候选时间窗（小时）
DIGEST_CANDIDATES_LIMIT = 200 # 快报送入 LLM 前截断
DIGEST_PER_CATEGORY     = 5   # 每分类最多几条

# ── LLM ───────────────────────────────────────────────
MMX_MODEL = os.environ.get("HERMES_MMX_MODEL", "MiniMax-M2.7-32K")
MMX_PROVIDER = os.environ.get("HERMES_MMX_PROVIDER", "minimax-cn")

# ── GitHub ─────────────────────────────────────────────
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")

# ── 健康检查阈值 ───────────────────────────────────────
CHECK_FETCH_HOURS = 2          # 抓取检查时间窗口（小时）
CHECK_FETCH_NEW_RECORDS_THRESHOLD = 1  # 低于此值告警
CHECK_REPORT_HOURS = 4         # 快报检查时间窗口（小时）
CHECK_REPORT_USED_THRESHOLD = 1 # 低于此值且有 pending 时告警

# ── 阶段耗时阈值（毫秒，超过则记录）────────────────────
PER_SOURCE_WARN_MS = 5000   # 单信源抓取超过此值记录 warn
SQLITE_WARN_MS      = 2000   # SQLite 操作超过此值记录 warn
LLM_WARN_MS         = 30000  # LLM 调用超过此值记录 warn

# ── 信源列表 ───────────────────────────────────────────
# 格式: (平台名, URL, 语言, 条数上限)
# URL="github" 时走 GitHub API
SOURCES = [
    ("IT之家",          "https://www.ithome.com/rss/",                "中文", 30),
    ("36kr",            "https://36kr.com/feed",                      "中文", 30),
    ("钛媒体",          "https://www.tmtpost.com/rss",                "中文", 18),
    ("爱范儿",          "https://www.ifanr.com/feed",                 "中文", 20),
    ("VentureBeat AI",  "https://venturebeat.com/category/ai/feed/",  "英文", 10),
    ("TechCrunch AI",   "https://techcrunch.com/category/artificial-intelligence/feed/", "英文", 20),
    ("MIT TR",          "https://www.technologyreview.com/feed/",      "英文", 10),
    ("MarkTechPost",     "https://www.marktechpost.com/feed/",          "英文", 10),
    ("Synced Review",    "https://syncedreview.com/feed/",             "英文", 10),
    ("KDnuggets",        "https://www.kdnuggets.com/feed",             "英文", 10),
    ("AI Trends",        "https://www.aitrends.com/feed/",            "英文", 10),
    ("AI News",          "https://artificialintelligence-news.com/feed/", "英文", 12),
    ("Wired",            "https://www.wired.com/feed/rss",             "英文", 30),
    ("Ars Technica",     "https://feeds.arstechnica.com/arstechnica/index", "英文", 20),
    ("Hacker News",      "https://hnrss.org/frontpage",                "英文", 20),
    ("Hacker News Show", "https://hnrss.org/show",                       "英文", 15),
    ("Quanta Magazine",   "https://www.quantamagazine.org/feed/",       "英文", 10),
    ("Google AI",        "https://blog.google/technology/ai/rss/",     "英文", 20),
    ("NVIDIA Blog",      "https://blogs.nvidia.com/feed/",             "英文", 20),
    ("OpenAI Blog",      "https://openai.com/blog/rss.xml",            "英文", 30),
    ("arXiv cs.AI",      "https://arxiv.org/rss/cs.AI",               "英文", 20),
    ("arXiv cs.CL",      "https://arxiv.org/rss/cs.CL",               "英文", 20),
    ("arXiv cs.LG",      "https://arxiv.org/rss/cs.LG",               "英文", 20),
    ("arXiv cs.CV",      "https://arxiv.org/rss/cs.CV",               "英文", 20),
    ("GitHub",           "github",                                     "英文", 15),
]

TAG_MAP = {
    "IT之家":"IT/数码/AI","36kr":"科技创投","钛媒体":"TMT深度","爱范儿":"产品/科技",
    "VentureBeat AI":"AI行业","TechCrunch AI":"科技AI","MIT TR":"AI深度",
    "MarkTechPost":"AI技术","Synced Review":"AI研究","KDnuggets":"数据科学/AI",
    "AI Trends":"AI趋势","AI News":"AI新闻","Wired":"科技文化","Ars Technica":"科技/AI",
    "Hacker News":"程序员社区","Hacker News Show":"Show HN 工具",
    "Quanta Magazine":"科学/AI","Google AI":"Google AI",
    "NVIDIA Blog":"NVIDIA官方","OpenAI Blog":"OpenAI官方",
    "arXiv cs.AI":"学术论文","arXiv cs.CL":"学术论文","arXiv cs.LG":"学术论文","arXiv cs.CV":"学术论文",
    "GitHub":"开源项目",
}

# ── 分类 emoji ─────────────────────────────────────────
CATEGORIES  = ['🚀', '🔥', '🏢', '🛠️', '💼']
CAT_LABELS  = {'🚀':'技术突破','🔥':'今日热点','🏢':'企业动态','🛠️':'工具/教程','💼':'商业模式','其他':'其他'}
