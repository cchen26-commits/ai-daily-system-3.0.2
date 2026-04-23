#!/usr/bin/env python3
"""
Hermes 本地 SQLite 主状态库。
db.py — 数据库操作原语（不包含业务逻辑）。
Source of truth: SQLite (HERMES_DB / runtime/shared_materials.db)
"""
import sqlite3, os, hashlib, re
from pathlib import Path
from datetime import datetime, timezone, timedelta

BJT = timezone(timedelta(hours=8))
DEFAULT_DB_PATH = Path(__file__).resolve().parents[1] / "runtime" / "shared_materials.db"
DB_PATH = os.environ.get(
    "HERMES_DB",
    str(DEFAULT_DB_PATH)
)

# ── Schema 版本 ─────────────────────────────────────────
SCHEMA_VERSION = 3  # materials 表当前 schema 版本

# ── 连接 ──────────────────────────────────────────────
def get_conn():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ── Schema Migration ──────────────────────────────────
_MIGRATIONS = {
    1: [
        # materials v1 初始建表
        """
        CREATE TABLE IF NOT EXISTS materials (
            id                    INTEGER PRIMARY KEY AUTOINCREMENT,
            title                 TEXT    NOT NULL,
            url                   TEXT,
            platform              TEXT,
            published_at          INTEGER,
            summary_raw           TEXT,
            category              TEXT,
            ai_relevance          TEXT,
            source_tier           TEXT,
            language              TEXT,
            region                TEXT,
            fingerprint          TEXT,
            hermes_status         TEXT    DEFAULT 'pending',
            hermes_selected_at    INTEGER,
            hermes_sent_at        INTEGER,
            openclaw_status       TEXT    DEFAULT 'pending',
            openclaw_selected_at  INTEGER,
            openclaw_doc_id       TEXT,
            openclaw_doc_url      TEXT,
            created_at            INTEGER NOT NULL,
            updated_at            INTEGER NOT NULL,
            source_fetch_batch    TEXT,
            sync_to_bitable_at    INTEGER,
            sync_status           TEXT,
            error_note            TEXT
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_fp        ON materials(fingerprint)",
        "CREATE INDEX IF NOT EXISTS idx_url      ON materials(url)",
        "CREATE INDEX IF NOT EXISTS idx_hermes   ON materials(hermes_status)",
        "CREATE INDEX IF NOT EXISTS idx_openclaw ON materials(openclaw_status)",
        "CREATE INDEX IF NOT EXISTS idx_pub_at   ON materials(published_at)",
    ],
    2: [
        # v2 新增字段
        "ALTER TABLE materials ADD COLUMN quality_score   INTEGER",
        "ALTER TABLE materials ADD COLUMN event_key        TEXT",
        "ALTER TABLE materials ADD COLUMN topic_cluster     TEXT",
        "ALTER TABLE materials ADD COLUMN content_type     TEXT",
        "ALTER TABLE materials ADD COLUMN ingest_version    TEXT",
        "ALTER TABLE materials ADD COLUMN last_scored_at    INTEGER",
        # 索引
        "CREATE INDEX IF NOT EXISTS idx_event_key   ON materials(event_key)",
        "CREATE INDEX IF NOT EXISTS idx_content_type ON materials(content_type)",
        "CREATE INDEX IF NOT EXISTS idx_quality     ON materials(quality_score)",
    ],
    3: [
        # v3: 新增 fetched_at — 本条记录进入 Hermes 候选池的时间
        # 用于时间窗口过滤，不应与 published_at（原文发布时间）混用
        "ALTER TABLE materials ADD COLUMN fetched_at INTEGER",
        "CREATE INDEX IF NOT EXISTS idx_fetched_at ON materials(fetched_at)",
    ],
}


def _get_schema_version(conn) -> int:
    """读取当前已记录的 schema 版本。"""
    try:
        row = conn.execute(
            "SELECT version FROM schema_migrations ORDER BY version DESC LIMIT 1"
        ).fetchone()
        return row["version"] if row else 0
    except Exception:
        return 0


def _record_migration(conn, version: int):
    conn.execute(
        "INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)",
        (version, int(datetime.now(BJT).timestamp() * 1000))
    )


def run_migrations():
    """
    执行 schema 迁移。migration 失败直接退出，不允许半迁移状态。
    """
    conn = get_conn()

    # 如果 schema_migrations 表不存在，先建
    conn.execute("""
        CREATE TABLE IF NOT EXISTS schema_migrations (
            version   INTEGER PRIMARY KEY,
            applied_at INTEGER NOT NULL
        )
    """)

    current = _get_schema_version(conn)
    applied = []
    for ver in sorted(_MIGRATIONS.keys()):
        if ver <= current:
            continue
        # 执行 migration
        for sql in _MIGRATIONS[ver]:
            try:
                conn.execute(sql)
            except sqlite3.Error as e:
                conn.close()
                print(f"[db] FATAL: migration v{ver} failed: {e}")
                raise SystemExit(1)
        _record_migration(conn, ver)
        conn.commit()
        applied.append(ver)
        print(f"  [db] migration v{ver} applied ({len(_MIGRATIONS[ver])} statements)")

    conn.close()
    return applied


# ── 初始化（建表 + 迁移）───────────────────────────────
def init_db():
    applied = run_migrations()
    if applied:
        print(f"[db] Applied migrations: {applied}")
    else:
        print(f"[db] Schema up to date (v{SCHEMA_VERSION})")


# ════════════════════════════════════════════════════════
# v2 字段计算函数（deterministic rule-based）
# ════════════════════════════════════════════════════════

_CONTENT_TYPE_PATTERNS = {
    # release — 官方发布/产品发布
    "release":   ["release", "launch", "unveil", "announce", "debut", "首发", "上线", "发布", "推出", "新品",
                   "正式开售", "开启预售", "上市", "公开发布", "新产品", "全新"],
    # research — 学术论文/研究报告
    "research":  ["paper", "arxiv", "study", "research", "benchmark", "survey",
                   "acl", "neurips", "icml", "iclr", "cvpr", "学术", "论文", "研究",
                   "technical report", "preprint", "findings"],
    # tool — 开源工具/SDK/CLI/API库/脚本/GitHub项目
    "tool":      [
                   # GitHub/开源生态
                   "github", "gitlab", "gitee", "bitbucket",
                   "open-source", "open source", "开源项目", "开源软件",
                   "library", "framework", "sdk", "cli", "api", "tool",
                   "repo", "repository", "stars ", "⭐",
                   # 包管理/安装
                   "pip install", "npm install", "pip:", "npm:", "cargo add",
                   "pip3", "poetry add", "uv add", "bundle add",
                   "apt install", "brew install", "yum install",
                   "install", "setup", "安装", "配置",
                   # 开发/代码
                   "github.com/", "github io", "github pages",
                   "docker image", "docker pull", "docker run",
                   "kubernetes", "helm chart", "kubectl",
                   "python", "javascript", "typescript", "rust", "go ", "golang",
                   "java ", "c++", ".py", ".js", ".ts", "node.js",
                   "script", "automation", "脚本", "自动化",
                   # AI开发工具
                   "langchain", "llamaindex", "crewai", "autogen",
                   "comfyui", "sd webui", "stable diffusion",
                   "ollama", "vllm", "transformers", "huggingface",
                   "rag", "vector db", "embedding",
                   # AI产品/平台
                   "chatbot", "copilot", "agent", "agent framework",
                   "prompt engineering", "提示词", "prompt",
                   "plugin", "extension", "addon", "浏览器插件",
                   "集成", "integration", "hub", "marketplace",
                   # 评测/基准
                   "benchmark", "evaluation", "leaderboard", "排行榜",
                   # 教程/实操类关键词
                   "how-to", "how to", "入门", "教程", "实战", "一步步",
                   "step by step", "getting started", "beginner's guide",
                   "tutorial", "guide", "learn",
                   "实操", "案例", "最佳实践", "best practice",
                   "workflow", "使用指南", "操作指南",
                   # 新工具发布（不仅仅是"release"）
                   "new tool", "open source tool", "free tool", "free api",
                   "launches", "launched", "open beta", "public beta",
                   "免费", "新工具", "工具发布",
                  ],
    # tutorial — 操作指南/how-to/实操案例
    "tutorial":  ["tutorial", "how to", "guide", "入门", "教程", "实战", "一步步",
                   "step by step", "learn", "实操", "案例",
                   "best practice", "workflow", "使用指南", "操作指南",
                   "cheatsheet", "cheat sheet", "速查表",
                   "课程", "培训", "上手", "从零开始", "从入门到精通",
                   "handson", "hands-on", "workshop"],
    # funding — 融资/投资
    "funding":   ["funding", "series", "raise", "invest", "vc", "seed", "angel",
                   "ipo", "acqui", "收购", "融资", "亿美元", "投资",
                   "a round", "b round", "c round", "pre-series",
                   "valuation", "市值", "估值", "轮融资", "领投", "跟投",
                   "investment", "raised", "raising", "fundraise"],
    # policy — 政府政策/监管/合规
    "policy":    ["policy", "regulation", "law", "ban", "restrict", "government",
                   "合规", "监管", "政策", "禁止", "法规", "白宫", "欧盟",
                   "部长", "国会", "议会", "fda", "sec ", "ftc",
                   "制裁", "列入实体清单", "出口管制", "审查", "安全审查"],
    # opinion — 观点/评论/分析
    "opinion":   ["opinion", "think", "believe", "view", "perspective",
                   "editorial", "commentary", "analysis", "观点", "看法", "评论", "思考",
                   "analysis", "拆解", "解读", "深度", "复盘"],
    # news — 商业/财经新闻（不包含上述特征的普通新闻）
    "news":      ["report", "news", "据报道", "获悉", "独家", "消息",
                   "公告", "财报", "业绩", "营收", "利润", "股价",
                   "市场", "行业", "收购", "合作", "战略", "合作",
                   "签约", "中标", "订单", "出货", "产能", "供应链"],
}


def compute_content_type(title: str, summary: str, platform: str) -> str:
    """
    规则化判断 content_type，优先级：
    1. 平台特征（如 GitHub→tool，arXiv→research）
    2. 标题/摘要关键词匹配
    无法判断 → news
    """
    text = (title + " " + summary).lower()

    # 平台特征
    if platform == "GitHub":
        return "tool"
    if "arXiv" in platform or platform == "Quanta Magazine":
        return "research"

    # 关键词匹配（按优先级）
    for ctype, kws in _CONTENT_TYPE_PATTERNS.items():
        if any(k in text for k in kws):
            return ctype

    return "news"


def compute_event_key(title: str, platform: str, published_at: int) -> str:
    """
    生成事件归并 key。
    规则：
    1. 提取主体名（公司/产品名）— 取标题前 1-2 个实词
    2. 提取核心关键词（去掉通用词后剩下的名词/动词）
    3. 日期窗口（48h，同一主体+同关键词→同一事件）
    4. 平台作为 secondary 区分（不同平台报道同一事件仍归并）
    """
    # 标准化标题
    raw = re.sub(r'[^\w\s\u4e00-\u9fa5]', ' ', title.lower())
    words = raw.split()
    # 过滤停用词
    stop = {'the','a','an','of','to','in','for','on','with','by','is','are','was','be','has','have','and','or','but','at','as','from','that','this','it','ai','new','top','best','how','what','why','when','where','which','who','open','source','google','microsoft','meta','apple','amazon','nvidia','openai','llm','model','gpt','gemini','claude'}
    keywords = [w for w in words if w not in stop and len(w) > 2]

    # 取前两个实体词作为主体
    entity = "_".join(keywords[:2]) if len(keywords) >= 2 else (keywords[0] if keywords else "unknown")

    # 日期窗口（48h，即两个 24h bucket）
    try:
        dt = datetime.fromtimestamp(published_at / 1000, tz=BJT)
        day_bucket = dt.strftime("%Y%m%d")
    except:
        day_bucket = "unknown"

    # hash 碰撞减少长度
    key_str = f"{entity}_{day_bucket}"
    return hashlib.md5(key_str.encode()).hexdigest()[:12]


# ── Category 重算函数（引入 content_type + source_tier）─────────
# 与 fetch_and_write.py 中 ai_classify 逻辑保持一致
_TECH_KW   = ['llm','gpt','gemini','claude','model','训练','fine-tun','rlhf','moe','scaling',
               'transformer','neural','网络结构','参数','benchmark','权重','涌现','agent','rag',
               'vector','embedding','diffusion','gans','pre-train','sft','ppo','rl',
               'attention','token','backprop','gradient','optimiz','loss function']
_HOT_KW    = ['发布','reveal','launch','announce','unveil','首发','曝光','release','debut',
               '推出','上线','新品','iphone','ipad','macbook','android','windows','surface',
               '氢能源车','无人机','机器人','半马','aio','手机','电脑','汽车',
               '特斯拉','比亚迪','小米','华为','苹果','oppo','vivo','三星']
_BIZ_KW    = ['invest','funding','series','raise','vc','seed','angel','ipo','acqui',
               '融资','亿美元','投资','招聘','高层','离职','加入',
               'runway','imagen','whisper','copilot','ceo','coo','cto','cfo',
               '合作','战略合作','签约','中标','订单','出货','产能',
               '扩张','裁员','破产','倒闭','诉讼','监管','罚款',
               '收购','并购','出售','剥离','分拆','上市']
# 商业模式关键词 — 涵盖收费/定价/商业化/变现/单位经济/平台经济
_MODEL_KW  = [
               # 核心商业概念
               '商业模式','盈利','收费','定价','订阅','付费','免费',
               '商业化','变现','广告','企业采购','b端','c端',
               'roi','return on','单位经济','unit economics',
               '市场份额','市场占有率','渗透率','覆盖率',
               # 财务指标
               '营收','营业收入','净利润','毛利率','净利率',
               '利润','利润率','亏损','扭亏','ipo','估值',
               '市值','股价','估值','融资金额','融资额',
               'arr','mrr','ltv','cac','burn rate',
               # 定价模型
               'api定价','推理成本','算力成本','训练成本',
               '定价策略','价格调整','提价','降价','涨价','降价',
               '套餐','年费','月费','订阅费','收费模式',
               'saas','software as a service','paas','iaas',
               '平台抽成','佣金','分成','代理商','渠道策略',
               # 生态/平台
               '生态合作','平台战略','生态伙伴','开发者生态',
               '平台化','生态系统','marketplace','ecosystem',
               # 增长/商业扩展
               '增长','下滑','下降','增长放缓','同比','环比',
               '商业落地','b2b','b2c','企业级','行业解决方案',
              ]
# 工具/教程关键词 — 仅限有明确可用性的工具/实操内容
# ⚠️ 泛编程语言名、泛 AI 词不等于"工具有价值"，需配合安装/使用信号使用
# 新闻平台（IT之家/36kr/钛媒体等）即使命中这些词，也需额外判断
_TOOL_KW   = [
               # GitHub/开源生态（明确的项目地址）
               'github.com/','github io','github pages',
               'open-source','open source','开源项目','开源软件','开源',
               'stars ','⭐','repository','repo ',
               # 明确的安装/使用命令
               'pip install','npm install','pip:','npm:','cargo add',
               'poetry add','uv add','apt install','brew install','yum install',
               'docker pull','docker run','docker image',
               # 明确的开发工具
               'sdk','cli','library','framework','plugin','extension',
               'langchain','llamaindex','crewai','autogen','dspy',
               'comfyui','sd webui','stable diffusion',
               'ollama','vllm','transformers','huggingface',
               'rag','vector db','embedding',
               'kubernetes','kubectl','helm chart',
               # 教程/实操结构
               'how-to','how to','tutorial','入门','教程','实战','一步步',
               'step by step','getting started','beginner',
               '实操','案例','workflow','使用指南','操作指南',
               'cheatsheet','速查表','课程','培训','上手','从零开始',
               'handson','hands-on','workshop',
               # 明确的项目/工具性标题格式
               'show hn:','launches','launched',
               'free api','public beta','open beta',
              ]


def _ai_classify_with_ct(title, summary, platform="", source_tier="", content_type="") -> str:
    """
    基于 content_type + source_tier + 关键词联合判断 category。
    决策顺序很重要：
    1. research → 技术突破
    2. tool/tutorial → 工具/教程（content_type 最优先）
    3. news/opinion + 工具特征 → 工具/教程（content_type 弱时用关键词兜底）
    4. funding/policy + 商业模式特征 → 商业模式（不直接跳企业动态）
    5. funding/policy → 企业动态（没有商业模式特征时）
    6. news + 商业模式特征 → 商业模式（在 news→今日热点 之前拦截）
    7. release → 技术突破 / 今日热点
    8. 技术/今日热点/企业动态关键词 → 对应类别
    9. 官方 → 今日热点
    10. news + 媒体/社区 → 今日热点
    11. 兜底 → 今日热点
    """
    text = (title + " " + summary).lower()

    # ── 1. research ──
    if content_type == "research":
        return "🚀技术突破"

    # ── 2. tool / tutorial（content_type 明确时优先）──
    if content_type in ("tool", "tutorial"):
        return "🛠️工具/教程"

    # ── 3. 开源工具来源 ──
    if source_tier == "开源工具":
        return "🛠️工具/教程"

    # ── 4. news/opinion + 工具关键词 → 工具/教程 ──
    # content_type 判不出 tool/tutorial 时，用关键词兜底
    # 位置在 news→今日热点 之前，避免被第10步拦截
    if content_type in ("news", "opinion") and any(k in text for k in _TOOL_KW):
        return "🛠️工具/教程"

    # ── 5. funding/policy 的双路径判断 ──
    if content_type in ("funding", "policy"):
        # 5a. 有商业模式特征 → 商业模式（不直接跳企业动态）
        if any(k in text for k in _MODEL_KW):
            return "💼商业模式"
        # 5b. 有商业/企业动态词 → 企业动态
        if any(k in text for k in _BIZ_KW):
            return "🏢企业动态"
        # 5c. 否则默认
        return "🏢企业动态"

    # ── 6. news + 商业模式特征 → 商业模式 ──
    # 位置在 news+媒体/社区→今日热点 之前
    if content_type == "news" and any(k in text for k in _MODEL_KW):
        return "💼商业模式"

    # ── 7. release 的细分 ──
    if content_type == "release":
        if source_tier == "官方":
            return "🔥今日热点"
        if any(k in text for k in _TECH_KW):
            return "🚀技术突破"
        return "🔥今日热点"

    # ── 8. 关键词兜底（按优先级）──
    if any(k in text for k in _TECH_KW):
        return "🚀技术突破"
    if any(k in text for k in _HOT_KW):
        return "🔥今日热点"
    if any(k in text for k in _BIZ_KW):
        return "🏢企业动态"

    # ── 9. 官方来源 ──
    if source_tier == "官方":
        return "🔥今日热点"

    # ── 10. news + 媒体/社区（不满足前述条件才走到这里）──
    if content_type == "news" and source_tier in ("媒体", "社区"):
        return "🔥今日热点"

    # ── 11. 兜底 ──
    return "🔥今日热点"


_SOURCE_TIER_SCORES = {
    "官方":  100,
    "研究":  90,
    "媒体":  70,
    "社区":  60,
    "开源工具": 80,
}
_AI_RELEVANCE_SCORES = {
    "高": 100,
    "中":  60,
    "低":  20,
    "非目标": 0,
}
_RECENT_HOURS = 24       # 24h 内满分时效
_RECENT_DAYS  = 3        # 3 天内线性衰减
_OLD_DAYS     = 7        # 7 天外保底分


def rate_quality(
    title: str,
    summary: str,
    source_tier: str,
    ai_relevance: str,
    published_at: int,
    fingerprint: str,
    content_type: str = "",
) -> tuple[int, int]:
    """
    计算 quality_score（0-100）和 last_scored_at（ms）。
    维度：
      - source_tier 权重（20%）
      - ai_relevance 权重（20%）
      - 发布时间时效性（25%）
      - 标题/摘要信息密度（20%）
      - 非 AI / 泛科技内容降权（10%）
      - 重复/近重复惩罚（5%）
      - 工具/教程 bonus（+5）：content_type 为 tool/tutorial 时
    返回 (quality_score, last_scored_at_ms)
    """
    now_ms = int(datetime.now(BJT).timestamp() * 1000)

    # 1. source_tier（满分 100 × 20% = 20）
    tier_score    = _SOURCE_TIER_SCORES.get(source_tier, 50)
    tier_component = tier_score * 0.20

    # 2. ai_relevance（满分 100 × 20% = 20）
    rel_score     = _AI_RELEVANCE_SCORES.get(ai_relevance, 0)
    rel_component = rel_score * 0.20

    # 3. 时效性（满分 100 × 25% = 25）
    try:
        age_hours = (now_ms - published_at) / (1000 * 3600)
    except:
        age_hours = 9999
    if age_hours <= _RECENT_HOURS:
        time_score = 100
    elif age_hours <= _RECENT_DAYS * 24:
        time_score = max(40, 100 - (age_hours - _RECENT_HOURS) * (60 / ((_RECENT_DAYS * 24) - _RECENT_HOURS)))
    else:
        time_score = max(15, 40 - (age_hours - _RECENT_DAYS * 24) * (25 / (_OLD_DAYS * 24 - _RECENT_DAYS * 24)))
    time_component = time_score * 0.25

    # 4. 信息密度（标题 + 摘要长度，20%）
    density = min(100, (len(title) * 2 + len(summary)) / 10)
    density_component = density * 0.20

    # 5. 非 AI / 泛科技降权（10%）
    text = (title + " " + summary).lower()
    ai_specific_kws = ['llm','gpt','gemini','claude','model','training','fine-tun','rlhf','moe','scaling',
                        'transformer','neural','agent','rag','vector','embedding','diffusion','gans',
                        'reinforcement learning','supervised','unsupervised','generative','token',
                        '涌现','网络结构','参数','权重','benchmark','agent']
    generic_kws = ['tech','手机','电脑','汽车','游戏','评测','互联网','软件','电脑','笔记本','相机']
    has_ai = any(k in text for k in ai_specific_kws)
    is_generic = any(k in text for k in generic_kws) and not has_ai
    if is_generic:
        relevance_penalty = 0.10
    elif not has_ai:
        relevance_penalty = 0.05
    else:
        relevance_penalty = 0.0
    relevance_component = (1 - relevance_penalty)  # 乘子

    # 6. 近重复惩罚（5%）— fingerprint 前 6 位相同 → -10
    fp_prefix = fingerprint[:6] if len(fingerprint) >= 6 else fingerprint
    dup_kw = ['_copy','_update','_v2','_2','duplicate']
    is_dup = any(d in title.lower() for d in dup_kw)
    dup_penalty = 0.05 if is_dup else 0.0

    total = (tier_component + rel_component + time_component + density_component) * relevance_component
    total = max(0, min(100, total - dup_penalty * 100))
    # 工具/教程 bonus：真·工具/教程（content_type 明确为 tool/tutorial）加 5 分
    if content_type in ("tool", "tutorial"):
        total = min(100, total + 5)
    return (round(total), now_ms)


# ── 写入 ──────────────────────────────────────────────
def upsert_material(batch_id: str, records: list) -> dict:
    """
    批量插入或忽略已存在(URL 或 fingerprint 重复)的素材。
    v2: 同时写入 quality_score / event_key / content_type / ingest_version / last_scored_at。
    返回: {"inserted": N, "skipped": N, "errors": N, "inserted_records": [r, ...]}
    """
    conn = get_conn()
    inserted_list = []
    skipped = errors = 0
    now_ms = int(datetime.now(BJT).timestamp() * 1000)

    for r in records:
        try:
            if not (r.get("title") or "").strip():
                skipped += 1; continue
            if not (r.get("url") or "").strip():
                skipped += 1; continue

            url = r["url"].strip()
            fp  = r.get("fingerprint") or hashlib.md5(
                re.sub(r"[^\w]", "", (r.get("title") or "").lower()).encode()
            ).hexdigest()[:16]

            cur = conn.execute(
                "SELECT id FROM materials WHERE url=? OR fingerprint=? LIMIT 1",
                (url, fp)
            ).fetchone()
            if cur:
                skipped += 1; continue

            # v2 新字段计算（category 也在此用 content_type 重新对齐）
            # content_type 需先算，用于 rate_quality 加成和 category 判断
            content_type = compute_content_type(
                r.get("title", ""),
                r.get("summary_raw", ""),
                r.get("platform", ""),
            )
            quality, scored_at = rate_quality(
                title        = r.get("title", ""),
                summary      = r.get("summary_raw", ""),
                source_tier  = r.get("source_tier", ""),
                ai_relevance = r.get("ai_relevance", ""),
                published_at = r.get("published_at") or 0,
                fingerprint  = fp,
                content_type = content_type,
            )
            # 重新用 content_type + source_tier 计算 category
            category = _ai_classify_with_ct(
                r.get("title", ""),
                r.get("summary_raw", ""),
                r.get("platform", ""),
                r.get("source_tier", ""),
                content_type,
            )
            event_key = compute_event_key(
                r.get("title", ""),
                r.get("platform", ""),
                r.get("published_at") or 0,
            )

            conn.execute("""
                INSERT INTO materials (
                    title, url, platform, published_at, summary_raw,
                    category, ai_relevance, source_tier, language, region, fingerprint,
                    hermes_status, created_at, updated_at, source_fetch_batch, fetched_at,
                    quality_score, event_key, content_type, ingest_version, last_scored_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, (
                (r.get("title") or "")[:500],
                url,
                r.get("platform", ""),
                r.get("published_at") or 0,
                (r.get("summary_raw") or "")[:1000],
                category,
                r.get("ai_relevance", ""),
                r.get("source_tier", ""),
                r.get("language", ""),
                r.get("region", ""),
                fp,
                "pending",
                now_ms, now_ms,
                batch_id,
                now_ms,           # fetched_at = 进入候选池时间 = now
                quality,
                event_key,
                content_type,
                "v2",
                scored_at,
            ))
            inserted_list.append(r)
        except Exception:
            errors += 1

    conn.commit(); conn.close()
    return {
        "inserted": len(inserted_list),
        "skipped": skipped,
        "errors": errors,
        "inserted_records": inserted_list,
    }


# ── Hermes 快报候选读取 ────────────────────────────────
def get_hermes_candidates(hours: int = 4, limit: int = 200) -> list:
    """
    读取 Hermes 快报候选记录。
    时间窗口使用 fetched_at（进入候选池时间），而非 published_at（原文发布时间），
    确保老 RSS 条目在重新入池时仍能进入当日日报。
    """
    conn = get_conn()
    cutoff = int((datetime.now(BJT) - timedelta(hours=hours)).timestamp() * 1000)
    rows = conn.execute("""
        SELECT id, title, url, platform, published_at, summary_raw,
               category, ai_relevance, source_tier, language
        FROM materials
        WHERE hermes_status = 'pending'
          AND fetched_at > ?
        ORDER BY fetched_at DESC
        LIMIT ?
    """, (cutoff, limit)).fetchall()
    conn.close()
    return [dict(row) for row in rows]


# ── Hermes 状态更新 ──────────────────────────────────
def mark_hermes_used(material_ids: list) -> int:
    if not material_ids:
        return 0
    conn = get_conn()
    now_ms = int(datetime.now(BJT).timestamp() * 1000)
    conn.execute(
        "UPDATE materials SET hermes_status='used', hermes_sent_at=?, updated_at=? "
        "WHERE id IN (%s)" % ",".join("?" * len(material_ids)),
        [now_ms, now_ms] + list(material_ids)
    )
    count = conn.total_changes; conn.commit(); conn.close()
    return count


def mark_hermes_ignored(material_ids: list) -> int:
    if not material_ids:
        return 0
    conn = get_conn()
    now_ms = int(datetime.now(BJT).timestamp() * 1000)
    conn.execute(
        "UPDATE materials SET hermes_status='ignored', updated_at=? "
        "WHERE id IN (%s)" % ",".join("?" * len(material_ids)),
        [now_ms] + list(material_ids)
    )
    count = conn.total_changes; conn.commit(); conn.close()
    return count


# ── OpenClaw 状态更新 ─────────────────────────────────
def update_openclaw_status(material_id: int, status: str, doc_id: str = "", doc_url: str = "") -> bool:
    conn = get_conn()
    now_ms = int(datetime.now(BJT).timestamp() * 1000)
    conn.execute("""
        UPDATE materials
        SET openclaw_status=?, openclaw_selected_at=?,
            openclaw_doc_id=?, openclaw_doc_url=?, updated_at=?
        WHERE id=?
    """, (status, now_ms, doc_id, doc_url, now_ms, material_id))
    ok = conn.total_changes > 0
    conn.commit(); conn.close()
    return ok


# ── 同步审计 ─────────────────────────────────────────
def mark_synced(material_ids: list, status: str = "ok", error_note: str = ""):
    if not material_ids:
        return
    conn = get_conn()
    now_ms = int(datetime.now(BJT).timestamp() * 1000)
    placeholders = ",".join("?" * len(material_ids))
    conn.execute(
        f"UPDATE materials SET sync_status=?, sync_to_bitable_at=?, error_note=?, updated_at=? "
        f"WHERE id IN ({placeholders})",
        [status, now_ms, error_note, now_ms] + list(material_ids)
    )
    conn.commit(); conn.close()


# ── 监控专用查询 ──────────────────────────────────────

def check_fetch_stats(hours: int = 2) -> dict:
    conn = get_conn()
    now_ms = int(datetime.now(BJT).timestamp() * 1000)
    cutoff_ms = int((datetime.now(BJT) - timedelta(hours=hours)).timestamp() * 1000)

    row = conn.execute("""
        SELECT COUNT(*) as cnt FROM materials WHERE created_at >= ?
    """, (cutoff_ms,)).fetchone()
    new_records = row["cnt"] if row else 0

    last_batch_row = conn.execute("""
        SELECT source_fetch_batch as batch_id, MAX(created_at) as ts
        FROM materials
        WHERE source_fetch_batch IS NOT NULL AND source_fetch_batch != ''
        GROUP BY source_fetch_batch
        ORDER BY ts DESC LIMIT 1
    """).fetchone()

    last_batch_id = last_batch_row["batch_id"] if last_batch_row else None
    last_fetch_ts = last_batch_row["ts"] if last_batch_row else None

    batch_recent = 0
    if last_fetch_ts and last_fetch_ts >= cutoff_ms:
        batch_recent = conn.execute("""
            SELECT COUNT(*) as cnt FROM materials WHERE source_fetch_batch = ?
        """, (last_batch_id,)).fetchone()["cnt"]

    pending_row = conn.execute("""
        SELECT COUNT(*) as cnt FROM materials WHERE hermes_status='pending'
    """).fetchone()
    pending = pending_row["cnt"] if pending_row else 0

    conn.close()
    return {
        "hours": hours,
        "new_records": new_records,
        "last_batch_id": last_batch_id,
        "last_fetch_ts": last_fetch_ts,
        "batch_record_count": batch_recent,
        "pending": pending,
        "checked_at": now_ms,
    }


def check_report_stats(hours: int = 4) -> dict:
    conn = get_conn()
    now_ms = int(datetime.now(BJT).timestamp() * 1000)
    cutoff_ms = int((datetime.now(BJT) - timedelta(hours=hours)).timestamp() * 1000)

    used_row = conn.execute("""
        SELECT COUNT(*) as cnt FROM materials
        WHERE hermes_status = 'used'
          AND hermes_sent_at IS NOT NULL
          AND hermes_sent_at >= ?
    """, (cutoff_ms,)).fetchone()
    recent_used = used_row["cnt"] if used_row else 0

    pending_row = conn.execute("""
        SELECT COUNT(*) as cnt FROM materials WHERE hermes_status='pending'
    """).fetchone()
    pending = pending_row["cnt"] if pending_row else 0

    last_sent_row = conn.execute("""
        SELECT MAX(hermes_sent_at) as last_sent FROM materials
        WHERE hermes_sent_at IS NOT NULL
    """).fetchone()
    last_sent_at = last_sent_row["last_sent"] if last_sent_row else None

    conn.close()
    return {
        "hours": hours,
        "recent_used": recent_used,
        "pending": pending,
        "last_sent_at": last_sent_at,
        "has_recent_used": recent_used > 0,
        "checked_at": now_ms,
    }


# ── 统计 ──────────────────────────────────────────────
def stats() -> dict:
    conn = get_conn()
    cur = conn.execute("""
        SELECT
            COUNT(*)                                      as total,
            SUM(CASE WHEN hermes_status='pending'    THEN 1 ELSE 0 END) as pending,
            SUM(CASE WHEN hermes_status='used'      THEN 1 ELSE 0 END) as used,
            SUM(CASE WHEN hermes_status='ignored'    THEN 1 ELSE 0 END) as ignored,
            SUM(CASE WHEN openclaw_status='pending'  THEN 1 ELSE 0 END) as oc_pending,
            SUM(CASE WHEN openclaw_status='published' THEN 1 ELSE 0 END) as oc_published,
            SUM(CASE WHEN sync_status='failed'       THEN 1 ELSE 0 END) as sync_failed
        FROM materials
    """).fetchone()
    conn.close()
    return dict(cur)


if __name__ == "__main__":
    init_db()
    print(f"DB init OK: {DB_PATH}")
    s = stats()
    print(f"   Total:{s['total']} | pending:{s['pending']} | used:{s['used']} | "
          f"ignored:{s['ignored']} | oc_pending:{s['oc_pending']} | "
          f"oc_published:{s['oc_published']} | sync_failed:{s['sync_failed']}")
