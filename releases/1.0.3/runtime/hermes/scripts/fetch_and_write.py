#!/usr/bin/env python3
"""
抓取 + 写入本地 SQLite + 可选同步飞书多维表。
用法: python3 fetch_and_write.py
主流程：抓取 → 清洗 → 去重(fingerprint) → 分类/打标 → 写入 SQLite → 异步同步多维表
SQLite 是 source of truth。多维表同步失败不影响主流程。
"""
import urllib.request, xml.etree.ElementTree as ET, ssl, time, json, re, sys, subprocess, os, hashlib, functools
from datetime import datetime, timezone, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
sys.path.insert(0, str(RUNTIME_DIR))
import db as hermes_db
from db import compute_content_type
from config import (
    BJT, HERMES_DB, HERMES_LOGS,
    SOURCES, TAG_MAP,
)

# ── ID 生成 ───────────────────────────────────────────
RUN_ID = datetime.now(BJT).strftime("fetch_%Y%m%d_%H%M%S")

# ── 路径 ──────────────────────────────────────────────
os.environ["HERMES_DB"] = HERMES_DB
LOG_FILE     = os.path.join(HERMES_LOGS, "fetch.log")
RESULT_FILE  = os.path.join(HERMES_LOGS, f"{RUN_ID}.result.json")

# ── 结果记录（统一 schema）────────────────────────────
_result = {
    "schema_version": "2.0",
    "system": "hermes",
    "job_type": "fetch",
    "run_id": RUN_ID,
    "success": False,
    "status": "failed",         # ok | warning | no_content | failed
    "started_at": datetime.now(BJT).isoformat(),
    "finished_at": None,
    "duration_ms": None,
    "timezone": "Asia/Shanghai",
    "source_of_truth": "sqlite",
    "sqlite_path": HERMES_DB,
    "bitable_mode": "synced_view",
    "error": None,
    "warnings": [],
    "metrics": {
        "source_count": 0,
        "fetched_raw_count": 0,
        "filtered_invalid_count": 0,
        "inserted_count": 0,
        "skipped_duplicates": 0,
        "sqlite_total_after_run": 0,
        "sqlite_pending_after_run": 0,
        "sqlite_used_after_run": 0,
        "sqlite_ignored_after_run": 0,
        "sqlite_ok": False,
        "bitable_sync_ok": False,
        "bitable_synced_count": 0,
        "stage_durations_ms": {
            "fetch_total_ms": 0,
            "clean_ms": 0,
            "dedupe_ms": 0,
            "sqlite_write_ms": 0,
            "bitable_sync_ms": 0,
            "per_source_fetch_ms": {},
        },
    },
    "artifacts": {
        "fetch_batch_id": RUN_ID,
        "result_path": RESULT_FILE,
        "log_path": LOG_FILE,
    },
    "debug": {},
}

# ── 计时装饰器（同时收集 per_source）─────────────────
_per_src_ms = {}

def stage(name, per_source_key=None):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter_ns()
            try:
                return fn(*args, **kwargs)
            finally:
                ms = (time.perf_counter_ns() - t0) // 1_000_000
                _result["metrics"]["stage_durations_ms"][name] = ms
                if per_source_key:
                    _per_src_ms[per_source_key] = ms
                ts = datetime.now(BJT).strftime("%H:%M:%S.%f")[:-3]
                print(f"  [{ts}] {name}: {ms}ms")
        return wrapper
    return decorator

# ── 日志 ──────────────────────────────────────────────
_orig_print = print
def log(*args, **kwargs):
    kwargs.setdefault("flush", True)
    _orig_print(*args, **kwargs)
    try:
        with open(LOG_FILE, "a") as f:
            kwargs["file"] = f
            _orig_print(*args, **kwargs)
    except Exception:
        pass
print = log

# ── 分类 & 打标 ──────────────────────────────────────
# ai_classify 规则（按优先级）：
#  1. content_type=research → 技术突破（学术内容天然是技术突破）
#  2. content_type=funding / policy / company move → 企业动态
#  3. source_tier=官方 且 content_type=release → 今日热点（官方发布 ≠ 技术突破）
#  4. content_type=release + 技术类关键词 → 技术突破（产品发布/新版本）
#  5. content_type=release + 商业/产品关键词 → 今日热点（新品/商业发布）
#  6. source_tier=开源工具 或 content_type=tool/tutorial → 工具/教程
#  7. 技术突破关键词命中 → 技术突破
#  8. 今日热点关键词命中 → 今日热点（产品发布/活动）
#  9. 企业动态关键词命中 → 企业动态（融资/合作/收购/人事）
#  10. 商业模式关键词命中 → 商业模式（商业分析/战略/数据报告）
#  11. source_tier=官方 → 今日热点（官方动态默认算热点）
# 关键词分类 — 与 runtime/db.py 保持一致
# ── 技术突破关键词 ──
_TECH_KW   = ['llm','gpt','gemini','claude','model','训练','fine-tun','rlhf','moe','scaling',
               'transformer','neural','网络结构','参数','benchmark','权重','涌现','agent','rag',
               'vector','embedding','diffusion','gans','pre-train','sft','ppo','rl',
               'attention','token','backprop','gradient','optimiz','loss function']
# ── 今日热点关键词 ──
_HOT_KW    = ['发布','reveal','launch','announce','unveil','首发','曝光','release','debut',
               '推出','上线','新品','iphone','ipad','macbook','android','windows','surface',
               '氢能源车','无人机','机器人','半马','aio','手机','电脑','汽车',
               '特斯拉','比亚迪','小米','华为','苹果','oppo','vivo','三星']
# ── 企业动态关键词 ──
_BIZ_KW    = ['invest','funding','series','raise','vc','seed','angel','ipo','acqui',
               '融资','亿美元','投资','招聘','高层','离职','加入',
               'runway','imagen','whisper','copilot','ceo','coo','cto','cfo',
               '合作','战略合作','签约','中标','订单','出货','产能',
               '扩张','裁员','破产','倒闭','诉讼','监管','罚款',
               '收购','并购','出售','剥离','分拆','上市']
# ── 商业模式关键词 ──
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
# ── 工具/教程关键词 — 仅限有明确可用性的工具/实操内容
# ⚠️ 泛编程语言名、泛 AI 词不等于"工具有价值"，需配合安装/使用信号
# 新闻平台（IT之家/36kr/钛媒体等）即使命中这些词也需额外判断
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


def ai_classify(title, summary, platform="", source_tier="", content_type=""):
    """
    基于 content_type + source_tier + 关键词联合判断 category。
    与 runtime/db.py 的 _ai_classify_with_ct 保持一致。
    """
    text = (title + " " + summary).lower()

    # 1. research → 技术突破
    if content_type == "research":
        return "🚀技术突破"

    # 2. tool / tutorial → 工具/教程
    if content_type in ("tool", "tutorial"):
        return "🛠️工具/教程"

    # 3. 开源工具来源
    if source_tier == "开源工具":
        return "🛠️工具/教程"

    # 4. news/opinion + 工具关键词 → 工具/教程（content_type 弱时用关键词兜底）
    if content_type in ("news", "opinion") and any(k in text for k in _TOOL_KW):
        return "🛠️工具/教程"

    # 5. funding/policy → 双重路径（有商业模式特征则归商业模式）
    if content_type in ("funding", "policy"):
        if any(k in text for k in _MODEL_KW):
            return "💼商业模式"
        if any(k in text for k in _BIZ_KW):
            return "🏢企业动态"
        return "🏢企业动态"

    # 6. news + 商业模式特征 → 商业模式（在 news→今日热点 之前拦截）
    if content_type == "news" and any(k in text for k in _MODEL_KW):
        return "💼商业模式"

    # 7. release 细分
    if content_type == "release":
        if source_tier == "官方":
            return "🔥今日热点"
        if any(k in text for k in _TECH_KW):
            return "🚀技术突破"
        return "🔥今日热点"

    # 8. 关键词兜底
    if any(k in text for k in _TECH_KW):
        return "🚀技术突破"
    if any(k in text for k in _HOT_KW):
        return "🔥今日热点"
    if any(k in text for k in _BIZ_KW):
        return "🏢企业动态"

    # 9. 官方来源
    if source_tier == "官方":
        return "🔥今日热点"

    # 10. news + 媒体/社区（不满足前述条件才走到这里）
    if content_type == "news" and source_tier in ("媒体", "社区"):
        return "🔥今日热点"

    # 11. 兜底
    return "🔥今日热点"

def ai_relevance(title, summary):
    text = (title + ' ' + summary).lower()
    if any(k in text for k in ['llm','gpt','gemini','claude','model','训练','fine-tun','rlhf','moe','scaling','transformer','权重','涌现','benchmark','涌现','agent','rag','vector']):
        return "高"
    if any(k in text for k in ['ai','machine learning','deep learning','neural','算法','模型','数据','robot','自动驾驶','生成式']):
        return "中"
    if any(k in text for k in ['tech','手机','电脑','汽车','游戏','评测','互联网','软件']):
        return "低"
    return "非目标"

def source_tier(platform):
    if platform in ('Google AI','NVIDIA Blog','OpenAI Blog','Ars Technica'):
        return "官方"
    if platform in ('VentureBeat AI','TechCrunch AI','MIT TR','Wired','爱范儿','钛媒体','36kr','IT之家'):
        return "媒体"
    if 'arXiv' in platform or platform == 'Quanta Magazine':
        return "研究"
    if platform in ('Hacker News','Hacker News Show','AI News','AI Trends','MarkTechPost','Synced Review','KDnuggets'):
        return "社区"
    if platform == 'GitHub':
        return "开源工具"
    return "媒体"

def normalize_title(title):
    s = title.lower().strip()
    s = re.sub(r'\s*[（(]via[）)]*\s*', '', s)
    s = re.sub(r'[^\w\u4e00-\u9fa5]', '', s)
    return s

def fingerprint(title):
    return hashlib.md5(normalize_title(title).encode('utf-8')).hexdigest()[:16]

# ── 抓取 ──────────────────────────────────────────────
@stage("fetch_total", per_source_key="__total__")
def fetch_all():
    all_raw = []
    source_ok = 0
    for name, url, lang, limit in SOURCES:
        t0 = time.perf_counter_ns()
        recs = fetch_github(lang, limit) if url == 'github' else fetch_rss(name, url, lang, limit)
        elapsed = (time.perf_counter_ns() - t0) // 1_000_000
        _per_src_ms[name] = elapsed
        if recs:
            source_ok += 1
        for r in recs:
            # 先算 content_type，再用于 category 判断
            r["content_type"]  = compute_content_type(r["title"], r["summary_raw"], r["platform"])
            r["source_tier"]   = source_tier(r["platform"])
            r["category"]      = ai_classify(r["title"], r["summary_raw"],
                                             platform=r["platform"],
                                             source_tier=r.get("source_tier",""),
                                             content_type=r["content_type"])
            r["ai_relevance"] = ai_relevance(r["title"], r["summary_raw"])
            r["fingerprint"]   = fingerprint(r["title"])
        all_raw.extend(recs)
        time.sleep(0.2)
    _result["metrics"]["source_count"] = source_ok
    return all_raw

@stage("clean")
def clean_filter(records):
    before = len(records)
    filtered = [r for r in records if r["title"].strip() and r["url"].strip()]
    _result["metrics"]["fetched_raw_count"] = before
    _result["metrics"]["filtered_invalid_count"] = before - len(filtered)
    return filtered

@stage("sqlite_write")
def write_sqlite(records):
    result = hermes_db.upsert_material(RUN_ID, records)
    _result["metrics"]["inserted_count"]    = result["inserted"]
    _result["metrics"]["skipped_duplicates"] = result["skipped"]
    _result["metrics"]["sqlite_ok"]         = True
    return result

# ── 主流程 ───────────────────────────────────────────
def fetch_rss(name, url, lang, limit=30):
    try:
        # hnrss.org 在部分机器 Python SSL 层连不通，单独走 curl
        if 'hnrss.org' in url:
            return _fetch_rss_curl(name, url, lang, limit)
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        resp = urllib.request.urlopen(req, timeout=8, context=ssl.create_default_context())
        root = ET.fromstring(resp.read().decode('utf-8', errors='ignore'))
        items = root.findall('.//item') or root.findall('.//entry')
        records = []
        for item in items[:limit]:
            title   = (item.findtext('title') or '').strip()
            link    = (item.findtext('link') or '').strip() or item.findtext('guid') or ''
            pub_str = item.findtext('pubDate') or item.findtext('published') or item.findtext('updated') or ''
            desc    = re.sub('<[^>]+>', '', (item.findtext('description') or item.findtext('summary') or ''))[:300]
            try:
                from email.utils import parsedate_to_datetime
                pub_dt = parsedate_to_datetime(pub_str.strip()) if pub_str else datetime.now(BJT)
            except:
                pub_dt = datetime.now(BJT)
            records.append({
                "title":         title[:200],
                "url":           link,
                "platform":      name,
                "published_at":  int(pub_dt.timestamp() * 1000),
                "summary_raw":   desc,
                "language":      lang,
                "region":        "中国" if lang == "中文" else "海外",
                "category":      TAG_MAP.get(name, "其他"),
            })
        return records
    except Exception:
        return []


def _fetch_rss_curl(name, url, lang, limit=30):
    """走 curl 获取 RSS（解决 hnrss.org 在 Python SSL 层连接失败的问题）"""
    try:
        r = subprocess.run(
            ['curl', '-s', '--max-time', '10', '-A', 'Mozilla/5.0', url],
            capture_output=True, text=True, timeout=12
        )
        if not r.stdout.strip():
            return []
        root = ET.fromstring(r.stdout.encode('utf-8', errors='ignore'))
        items = root.findall('.//item') or root.findall('.//entry')
        records = []
        for item in items[:limit]:
            title   = (item.findtext('title') or '').strip()
            link    = (item.findtext('link') or '').strip() or item.findtext('guid') or ''
            pub_str = item.findtext('pubDate') or item.findtext('published') or item.findtext('updated') or ''
            desc    = re.sub('<[^>]+>', '', (item.findtext('description') or item.findtext('summary') or ''))[:300]
            try:
                from email.utils import parsedate_to_datetime
                pub_dt = parsedate_to_datetime(pub_str.strip()) if pub_str else datetime.now(BJT)
            except:
                pub_dt = datetime.now(BJT)
            records.append({
                "title":         title[:200],
                "url":           link,
                "platform":      name,
                "published_at":  int(pub_dt.timestamp() * 1000),
                "summary_raw":   desc,
                "language":      lang,
                "region":        "中国" if lang == "中文" else "海外",
                "category":      TAG_MAP.get(name, "其他"),
            })
        return records
    except Exception:
        return []

# ── GitHub ─────────────────────────────────────────────
def fetch_github(lang, limit=15):
    try:
        req = urllib.request.Request(
            "https://api.github.com/search/repositories?q=AI+created:>2026-04-10&sort=stars&order=desc&per_page=15",
            headers={"User-Agent": "Mozilla/5.0","Accept":"application/vnd.github.v3+json"}
        )
        resp = urllib.request.urlopen(req, timeout=8, context=ssl.create_default_context())
        data = json.loads(resp.read().decode())
        records = []
        for item in data.get('items', [])[:limit]:
            created = datetime.fromisoformat(item['created_at'].replace('Z','+00:00')).timestamp()
            records.append({
                "title":         f"{item['full_name']} ⭐{item.get('stargazers_count',0)}",
                "url":           item.get('html_url',''),
                "platform":      "GitHub",
                "published_at":  int(created * 1000),
                "summary_raw":   (item.get('description') or '')[:300],
                "language":      lang,
                "region":        "海外",
                "category":      "开源项目",
            })
        return records
    except:
        return []

# ── 写 result ──────────────────────────────────────────
def write_result():
    _result["finished_at"] = datetime.now(BJT).isoformat()
    stage_ms = _result["metrics"]["stage_durations_ms"]
    stage_ms["fetch_total_ms"] = stage_ms.pop("fetch_total", 0)
    stage_ms["per_source_fetch_ms"] = dict(_per_src_ms)
    _result["duration_ms"] = sum(v for v in stage_ms.values() if isinstance(v, (int, float)))
    if isinstance(stage_ms.get("per_source_fetch_ms"), dict):
        total_check = stage_ms["per_source_fetch_ms"].get("__total__", 0)
        if total_check == 0:
            stage_ms["per_source_fetch_ms"]["__total__"] = stage_ms.get("fetch_total_ms", 0)

    # 状态判定
    if not _result["metrics"]["sqlite_ok"]:
        _result["status"] = "failed"
    elif _result["metrics"]["inserted_count"] == 0 and _result["metrics"]["skipped_duplicates"] > 0:
        _result["status"] = "no_content"
    elif _result["warnings"]:
        _result["status"] = "warning"
    else:
        _result["status"] = "ok"

    _result["success"] = _result["status"] in ("ok", "warning", "no_content")

    try:
        with open(RESULT_FILE, "w", encoding="utf-8") as f:
            json.dump(_result, f, ensure_ascii=False, indent=2)
        print(f"  Result → {RESULT_FILE}")
    except Exception as e:
        print(f"  Result 写入失败: {e}")

# ── 主流程 ────────────────────────────────────────────
def main():
    ts = datetime.now(BJT).strftime('%Y-%m-%d %H:%M')
    print(f"[{ts}] 抓取开始 (run={RUN_ID})")

    hermes_db.init_db()

    all_raw     = fetch_all()
    filtered    = clean_filter(all_raw)
    write_sqlite(filtered)

    # DB 统计快照
    st = hermes_db.stats()
    _result["metrics"]["sqlite_total_after_run"]   = st["total"]
    _result["metrics"]["sqlite_pending_after_run"] = st["pending"]
    _result["metrics"]["sqlite_used_after_run"]    = st["used"]
    _result["metrics"]["sqlite_ignored_after_run"]  = st["ignored"]

    write_result()

    print(f"  DB: 总={st['total']} | pending={st['pending']} | used={st['used']} | ignored={st['ignored']}")
    print(f"[{datetime.now(BJT).strftime('%Y-%m-%d %H:%M')}] 完成")

if __name__ == "__main__":
    main()
