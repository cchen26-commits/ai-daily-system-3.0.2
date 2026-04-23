#!/usr/bin/env python3
"""
生成精华版日报并通过 Hermes CLI 发送。
主流程（SQLite-first）：
  从 SQLite 读取 hermes_status=pending 的候选
  → AI 挑选
  → 发送
  → 更新 SQLite hermes_status=used
  → 可选同步多维表
多维表同步失败不影响快报发送成功判定。
"""
import subprocess, sys, os, json, re, time, functools
from datetime import datetime, timezone, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
sys.path.insert(0, str(RUNTIME_DIR))
import db as hermes_db
from config import (
    BJT, HERMES_DB, HERMES_LOGS,
    BITABLE_SYNC_ENABLED,
    CATEGORIES, CAT_LABELS,
    MMX_MODEL, MMX_PROVIDER,
    DIGEST_CANDIDATES_HOURS, DIGEST_CANDIDATES_LIMIT,
)

# ── ID 生成 ───────────────────────────────────────────
RUN_ID = datetime.now(BJT).strftime("hermes_%Y%m%d_%H%M%S")
DRY_RUN = False  # 已验证 RID+assemble 架构，真实发送已启用

# ── 路径 ──────────────────────────────────────────────
os.environ["HERMES_DB"] = HERMES_DB
LOG_FILE           = os.path.join(HERMES_LOGS, "report.log")
OUTPUT_PATH        = os.path.join(HERMES_LOGS, f"report_{RUN_ID}.md")
RESULT_FILE  = os.path.join(HERMES_LOGS, f"digest_{RUN_ID}.result.json")
LAST_REPORT_PATH   = os.path.join(HERMES_LOGS, "last_sent_titles.json")

# ── 结果记录（统一 schema）────────────────────────────
_result = {
    "schema_version": "1.0",
    "system": "hermes",
    "job_type": "digest",
    "run_id": RUN_ID,
    "success": False,
    "status": "failed",
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
        "candidates_before_filter": 0,
        "candidates_after_filter": 0,
        "candidates_after_dedup": 0,
        "candidates_for_llm": 0,
        "selected_count": 0,
        "sqlite_ok": False,
        "send_ok": False,
        "bitable_sync_ok": False,
        "recent_pending_after_run": 0,
        "recent_used_after_run": 0,
        "stage_durations_ms": {
            "sqlite_read_ms": 0,
            "filter_ms": 0,
            "dedupe_ms": 0,
            "llm_ms": 0,
            "send_ms": 0,
            "sqlite_writeback_ms": 0,
            "bitable_sync_ms": 0,
        },
    },
    "artifacts": {
        "output_md_path": OUTPUT_PATH,
        "selected_record_ids": [],
        "last_sent_snapshot_path": LAST_REPORT_PATH,
        "result_path": RESULT_FILE,
        "log_path": LOG_FILE,
    },
    "debug": {},
}

# ── 计时装饰器 ────────────────────────────────────────
def stage(name):
    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            t0 = time.perf_counter_ns()
            try:
                return fn(*args, **kwargs)
            finally:
                ms = (time.perf_counter_ns() - t0) // 1_000_000
                _result["metrics"]["stage_durations_ms"][name] = ms
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

# ── SQLite ────────────────────────────────────────────
@stage("sqlite_read")
def load_candidates(hours: int, limit: int) -> list:
    candidates = hermes_db.get_hermes_candidates(hours=hours, limit=limit)
    _result["metrics"]["candidates_before_filter"] = len(candidates)
    print(f"  SQLite 读取候选: {len(candidates)} 条 (最近 {hours}h, limit={limit})")
    return candidates

@stage("dedupe")
def dedupe(records: list, dup_titles: set) -> list:
    """基于 normalize 标题去重（窗口内重复 + 上轮已发）"""
    before = len(records)
    seen_norm = set()
    unique = []
    for r in records:
        norm = normalize_title(r.get('title', ''))
        if norm not in seen_norm and norm not in dup_titles:
            seen_norm.add(norm); unique.append(r)
    _result["metrics"]["candidates_after_filter"] = before
    _result["metrics"]["candidates_after_dedup"] = len(unique)
    _result["metrics"]["candidates_for_llm"] = len(unique)
    print(f"  去重后候选: {len(unique)} 条 (去重 {before - len(unique)} 条)")
    return unique

MAX_PER_CATEGORY = 8   # 每个分类最多给 LLM N 条

@stage("build_sections")
def build_sections(records: list, dup_titles: set) -> tuple:
    """
    返回 (sections_dict, rid_to_record_dict)
    - 每个分类最多 MAX_PER_CATEGORY 条（预筛截断）
    - 摘要缩短至 50 字以内
    - 格式：RID_xxx | 标题 | src:来源 | qs:等级
    """
    sections = {cat: [] for cat in CATEGORIES}
    sections['其他'] = []
    rid_map = {}
    seen_norm = set()

    for r in records:
        norm = normalize_title(r.get('title', ''))
        if norm in seen_norm or norm in dup_titles:
            continue
        seen_norm.add(norm)

        rid = f"RID_{r['id']}"
        rid_map[rid] = r
        cat = extract_emoji(r.get('category', ''))
        if cat not in CATEGORIES:
            cat = '其他'
        # 摘要缩短到 50 字
        summary = (r.get('summary_raw') or '')[:50]
        if len(r.get('summary_raw', '')) > 50:
            summary += '…'
        # 格式：RID_xxx | 标题 | src:来源 | qs:等级 [摘要]
        line = f"{rid} | {r.get('title', '')} | src:{r.get('platform', r.get('source_tier', '?'))} | qs:{r.get('ai_relevance', '?')}"
        if summary:
            line += f" | {summary}"
        sections[cat].append(line)

    # 脚本侧预筛：每分类最多 MAX_PER_CATEGORY 条
    for cat in CATEGORIES + ['其他']:
        if len(sections[cat]) > MAX_PER_CATEGORY:
            sections[cat] = sections[cat][:MAX_PER_CATEGORY]

    return sections, rid_map

def extract_emoji(cat_label: str) -> str:
    if not cat_label:
        return ''
    for c in CATEGORIES:
        if c in cat_label:
            return c
    return ''

def normalize_title(title: str) -> str:
    t = title.lower()
    # 移除所有引号变体（直弯撇引号等）
    t = re.sub(r"['\"'`\u2018\u2019\u201c\u201d\u2032\u2033]", '', t)
    # 移除标点符号，保留字母数字中文
    t = re.sub(r'[%$()【】\[\]「」『』《》〈〉\-,\.\—:;!?。，、；：！？\s]+', ' ', t).strip()
    return t

def build_selection_prompt(sections: dict, date_str: str) -> str:
    """构建选择型 prompt（不包含标题/链接，LLM 只输出 RID + 分析）"""
    lines = [f"# AI 日报候选 | {date_str}\n"]
    for cat in CATEGORIES + ['其他']:
        if cat not in sections or not sections[cat]:
            continue
        label = CAT_LABELS.get(cat, cat)
        lines.append(f"\n## {cat} {label}\n")
        for item in sections[cat]:
            lines.append(item)

    prompt = f"""你是一个严格的事实型日报编辑。

【你的职责】
- 从候选中挑选值得关注的条目
- 为每个分类写简短分析
- 生成摘要（每条 1~2 句话）

【硬性规则 — 违反即整轮失败】
1. SELECTED 中只能输出候选编号（如 RID_917），不得输出标题、不得输出链接、不得输出任何其他内容
2. 禁止修改候选编号
3. 禁止创造不存在的编号
4. 禁止在 SELECTED 行输出任何标题或链接文字
5. 如果某个分类没有足够条目，可以少选或留空，绝不凭空创造
6. 严格遵守【工具/教程栏 专项约束】

【工具/教程栏 专项约束】
- 只选真正可用的工具（用户可以下载/访问/试用的），或有明确操作路径的教程
- 禁止选模型发布、评测对比、Benchmark、AI 趋势报道、政策/安全新闻
- 禁止选「公司宣布推出 XX 产品」类条目（那是企业动态）
- 禁止选「NSA/Google/某公司使用/部署 XX」类条目（那是新闻报道）
- 【产品/内测/公测公告不算工具】：即使标题里有「下载即用」「可体验」也不默认算工具，除非正文摘要里明确包含操作步骤
- 如果本轮没有足够合格的工具/教程，该栏可选 1~2 条甚至留空

【输出格式 — 严格遵守】

## SELECTED
🚧 技术前沿: RID_xxx, RID_yyy, RID_zzz
🛠️ 工具/教程: RID_aaa
🏢 企业动态: RID_bbb
📊 行业纵览: RID_ccc, RID_ddd
🔥 今日热点: RID_eee

（如某分类无合适条目，写「无」或直接省略该行）

## ANALYSIS
🚧 技术前沿：本分类入选理由及分析……
🛠️ 工具/教程：本分类入选理由及分析……
（每分类一段，没有入选的分类可以省略）

## SUMMARIES
RID_xxx: 摘要文本（LLM 根据摘要字段改写或精简，1~2句话）
RID_aab: 摘要文本
（每条入选条目一行，只写 RID_xxx 加冒号加摘要，不要写标题或链接）

{'='*50}
以下是今日候选条目（按分类组织）：
{chr(10).join(lines)}
{'='*50}

请严格按以上格式输出，不要输出任何格式以外的内容。"""
    return prompt

def dedupe_llm_output(content: str) -> str:
    """基于 normalize 标题去重，只保留每个标题第一次出现的条目"""
    lines = content.split('\n')
    seen_norm = set()
    deduped = []
    for line in lines:
        if line.strip().startswith('- ['):
            m = re.search(r'\[([^\]]+)\]', line)
            if m:
                norm = normalize_title(m.group(1))
                if norm in seen_norm:
                    continue
                seen_norm.add(norm)
        deduped.append(line)
    return '\n'.join(deduped)

def parse_llm_output(content: str) -> dict:
    """
    解析 LLM 输出，分离 SELECTED / ANALYSIS / SUMMARIES 三个区块。
    返回 {
        'selected': {'🚧 技术前沿': ['RID_xxx', ...], ...},
        'analysis': {'🚧 技术前沿': '分析文本', ...},
        'summaries': {'RID_xxx': '摘要文本', ...}
    }
    """
    result = {
        'selected': {},
        'analysis': {},
        'summaries': {}
    }

    # 分割三个区块
    lines = content.split('\n')
    current_section = None
    section_lines = []

    for line in lines:
        stripped = line.strip()
        if stripped == '## SELECTED':
            # 如果已经有内容，先处理之前的（防止 LLM 输出重复块）
            if current_section == 'selected' and section_lines:
                _parse_selected_block(result, section_lines)
            elif current_section == 'analysis' and section_lines:
                _parse_analysis_block(result, section_lines)
            current_section = 'selected'
            section_lines = []
        elif stripped == '## ANALYSIS':
            if current_section == 'selected' and section_lines:
                _parse_selected_block(result, section_lines)
            current_section = 'analysis'
            section_lines = []
        elif stripped == '## SUMMARIES':
            if current_section == 'analysis' and section_lines:
                _parse_analysis_block(result, section_lines)
            current_section = 'summaries'
            section_lines = []
        elif stripped.startswith('## '):
            current_section = None
        elif current_section:
            section_lines.append(stripped)

    # 处理最后一个区块
    if current_section == 'selected' and section_lines:
        _parse_selected_block(result, section_lines)
    elif current_section == 'analysis' and section_lines:
        _parse_analysis_block(result, section_lines)
    elif current_section == 'summaries' and section_lines:
        _parse_summaries_block(result, section_lines)

    return result

def _parse_selected_block(result: dict, lines: list):
    """解析 SELECTED 区块：🚧 技术前沿: RID_xxx, RID_yyy"""
    for line in lines:
        if ':' not in line:
            continue
        # 找到第一个冒号分割分类名和内容
        idx = line.index(':')
        cat_part = line[:idx].strip()
        content = line[idx+1:].strip()

        # 提取所有 RID_xxx
        rids = re.findall(r'RID_\d+', content)
        if rids:
            result['selected'][cat_part] = rids

def _parse_analysis_block(result: dict, lines: list):
    """解析 ANALYSIS 区块：🚧 技术前沿：分析文本..."""
    for line in lines:
        if '：' not in line and ':' not in line:
            continue
        sep = '：' if '：' in line else ':'
        idx = line.index(sep)
        cat_part = line[:idx].strip()
        text = line[idx+1:].strip()
        if cat_part and text:
            result['analysis'][cat_part] = text

def _parse_summaries_block(result: dict, lines: list):
    """解析 SUMMARIES 区块：RID_xxx: 摘要文本"""
    for line in lines:
        if ':' not in line:
            continue
        idx = line.index(':')
        rid = line[:idx].strip()
        text = line[idx+1:].strip()
        if rid.startswith('RID_'):
            result['summaries'][rid] = text

def assemble_report(parsed: dict, rid_map: dict, date_str: str) -> str:
    """
    根据 LLM 输出的 parsed 结构 + rid_map 回填真实字段，组装最终日报。
    标题/链接/来源 100% 来自 rid_map（不经 LLM）。
    """
    lines = [f"# AI 日报 | {date_str} 📡\n"]

    # 按固定顺序输出分类
    for cat in CATEGORIES + ['其他']:
        cat_label = CAT_LABELS.get(cat, cat)
        rids = parsed['selected'].get(cat, [])
        analysis = parsed['analysis'].get(cat, '')
        if not rids:
            continue

        lines.append(f"\n## {cat} {cat_label}\n")

        for rid in rids:
            rec = rid_map.get(rid)

def validate_llm_output_format(content: str) -> bool:
    """检查 LLM 输出是否包含禁止的事实字段（标题/链接）"""
    # 新协议下：LLM 只输出 RID，不输出 [标题](链接)
    # 如果出现了 markdown 链接格式，说明 LLM 违反了协议
    return '[' in content and '](' in content

def assemble_report(parsed: dict, rid_map: dict, date_str: str) -> str:
    """
    根据 LLM 输出的 parsed 结构 + rid_map 回填真实字段，组装最终日报。
    标题/链接/来源 100% 来自 rid_map（不经 LLM）。
    对 LLM 输出的分类名做容错匹配，支持近似 emoji。
    """
    # 容错映射：LLM 可能写的分类名 → 实际 CATEGORIES
    CAT_ALIASES = {
        '🚧': '🚀',  # LLM 误写技术前沿
        '📊': '💼',  # LLM 发明"行业纵览"，映射到商业模式
        '📱': '🔥',  # 未来可能的别名
    }

    def normalize_cat(key: str) -> str:
        """把 LLM 写的分类 key 映射到 CATEGORIES 中的标准 key"""
        for alias, canonical in CAT_ALIASES.items():
            if alias in key:
                return canonical
        # 直接匹配
        for c in CATEGORIES:
            if c in key:
                return c
        return None

    lines = [f"# AI 日报 | {date_str} 📡\n"]

    # 按固定顺序输出分类
    for cat in CATEGORIES + ['其他']:
        cat_label = CAT_LABELS.get(cat, cat)

        # 从 parsed['selected'] 里找匹配当前 cat 的 key
        matched_rids = []
        analysis = ''
        for llm_key, rids in parsed['selected'].items():
            norm = normalize_cat(llm_key)
            if norm == cat:
                matched_rids.extend(rids)
                if cat not in parsed['analysis']:
                    # 如果标准 cat 没分析文字，尝试用 LLM 的分析
                    analysis = parsed['analysis'].get(llm_key, '')
        if not matched_rids:
            continue

        lines.append(f"\n## {cat} {cat_label}\n")
        for rid in matched_rids:
            rec = rid_map.get(rid)
            if not rec:
                continue
            title = rec.get('title', '')
            url = rec.get('url', '')
            source = rec.get('platform', rec.get('source_tier', '?'))
            summary = parsed['summaries'].get(rid, rec.get('summary_raw', '')[:80])

            # 标题+链接 100% 来自数据库
            lines.append(f"- [{title}]({url})  [{source}]")
            if summary:
                lines.append(f"  → {summary}")

        if analysis:
            lines.append(f"\n  📝 {analysis}")

    return '\n'.join(lines)



@stage("llm")
def call_mmx_selection(prompt_text: str) -> str:
    print("  调用 AI 挑选...")
    model = os.environ.get("HERMES_MMX_MODEL", "MiniMax-M2.7-highspeed")
    env = {**os.environ, 'TERM': 'dumb'}
    result = subprocess.run(
        ['hermes', 'chat', '-q', prompt_text,
         '--provider', 'minimax-cn',
         '-m', model,
         '-Q', '--max-turns', '1'],
        capture_output=True, text=True,
        stdin=subprocess.DEVNULL,
        env=env,
        timeout=60
    )
    if result.returncode != 0:
        raise RuntimeError(f"hermes chat failed: {result.stderr}")
    raw = result.stdout.strip()
    # hermes -Q 输出格式：╭─ ⚕ Hermes ─ ... ─╮\n实际内容\n...\nsession_id: xxx
    # 提取实际内容行（去掉 box 框架和 session_id 行）
    lines = raw.split('\n')
    content_lines = [
        l for l in lines
        if not l.startswith('╭') and not l.startswith('╰')
        and not l.startswith('│') and not l.startswith('session_id')
        and not l.startswith('⚠')
    ]
    return '\n'.join(content_lines).strip()

@stage("send")
def send_via_hermes(content: str) -> bool:
    prompt = f"""将以下日报内容通过 feishu 发送给用户。内容为 Markdown 格式。
内容：
{content}
直接发送，不要复述或解释。如果发送失败请报告。"""
    env = {**os.environ, 'TERM': 'dumb'}
    result = subprocess.run(
        ['hermes', 'chat', '-q', prompt,
         '--provider', MMX_PROVIDER,
         '--source', 'cron', '-Q'],
        capture_output=True, text=True,
        stdin=subprocess.DEVNULL,
        env=env,
        timeout=120
    )
    return '已发送' in result.stdout or 'sent' in result.stdout.lower() or result.returncode == 0

@stage("sqlite_writeback")
def writeback_sqlite(all_records: list, selected_titles_norm: set):
    # 用去重后的实际发送内容标题来回写，不再依赖当前窗口候选
    # all_records = 当前窗口候选（可能不包含来自更早批次但被LLM选中的条目）
    # 所以改为：直接从DB所有pending记录里匹配
    import sqlite3 as sql
    conn = sql.connect(HERMES_DB)
    conn.row_factory = sql.Row
    cur = conn.cursor()
    matched_ids = []
    for norm in selected_titles_norm:
        # 全DB pending记录中匹配（不依赖当前窗口候选）
        cur.execute("""
            SELECT id FROM materials
            WHERE hermes_status = 'pending'
            AND LOWER(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
                REPLACE(title, '(', ''), ')', ''), ' ', ''), '-', ''), '。', ''), '，', ''))
                  = ?
            LIMIT 1
        """, (norm.replace(' ', ''),))
        row = cur.fetchone()
        if row:
            matched_ids.append(row['id'])
    conn.close()
    if matched_ids:
        count = hermes_db.mark_hermes_used(matched_ids)
        _result["artifacts"]["selected_record_ids"] = matched_ids
        print(f"  SQLite 更新 {count} 条为 used（搜全DB {len(matched_ids)} 条匹配）")
        return count
    return 0

# ── 写 result ──────────────────────────────────────────
def write_result():
    _result["finished_at"] = datetime.now(BJT).isoformat()
    _result["duration_ms"] = sum(
        v for v in _result["metrics"]["stage_durations_ms"].values()
        if isinstance(v, (int, float))
    )

    # 状态判定
    if not _result["metrics"]["sqlite_ok"]:
        _result["status"] = "failed"
    elif _result["metrics"]["candidates_before_filter"] == 0:
        _result["status"] = "no_content"
    elif _result["metrics"]["candidates_for_llm"] == 0:
        _result["status"] = "no_content"
    elif not _result["metrics"]["send_ok"]:
        _result["status"] = "failed"
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

# ── 辅助 ──────────────────────────────────────────────
def load_last_report_titles() -> set:
    if not os.path.exists(LAST_REPORT_PATH):
        return set()
    try:
        with open(LAST_REPORT_PATH, 'r', encoding='utf-8') as f:
            data = json.load(f)
        titles = data.get('titles', [])
        print(f"  上一份报告 {len(titles)} 条标题用于去重")
        return set(titles)
    except Exception:
        return set()

def save_last_report(titles: list):
    with open(LAST_REPORT_PATH, 'w', encoding='utf-8') as f:
        json.dump({'titles': titles, 'saved_at': datetime.now(BJT).isoformat()}, f, ensure_ascii=False)

# ── 主流程 ────────────────────────────────────────────
def main():
    now_bjt = datetime.now(BJT)
    date_str = now_bjt.strftime('%Y-%m-%d')
    print(f"[{now_bjt.strftime('%Y-%m-%d %H:%M')}] 日报开始 (run={RUN_ID})")

    hermes_db.init_db()
    _result["metrics"]["sqlite_ok"] = True

    records = load_candidates(hours=DIGEST_CANDIDATES_HOURS, limit=DIGEST_CANDIDATES_LIMIT)

    if not records:
        print("  SQLite 中无候选记录，退出")
        _result["status"] = "no_content"
        _result["success"] = True
        _result["error"] = None
        write_result()
        sys.exit(0)

    dup_titles = load_last_report_titles()
    unique_records = dedupe(records, dup_titles)
    sections, rid_map = build_sections(unique_records, dup_titles)

    # 检查是否有有效分类
    has_content = any(sections.get(cat) for cat in CATEGORIES + ['其他'])
    if not has_content:
        print("  去重后无候选内容，退出")
        _result["status"] = "no_content"
        _result["success"] = True
        write_result()
        sys.exit(0)

    prompt_text = build_selection_prompt(sections, date_str)

    selected_raw = call_mmx_selection(prompt_text)
    if not selected_raw:
        _result["error"] = {"code": "LLM_FAILED", "message": "AI 挑选无返回", "stage": "llm", "retryable": True}
        _result["status"] = "failed"
        write_result()
        sys.exit(1)

    # 保存 LLM 原始输出供调试
    llm_raw_path = os.path.join(HERMES_LOGS, f"llm_raw_{RUN_ID}.txt")
    with open(llm_raw_path, 'w', encoding='utf-8') as f:
        f.write(selected_raw)
    print(f"  LLM 原始输出已保存: {llm_raw_path}")

    # ── 解析 LLM 输出 ────────────────────────────────────
    parsed = parse_llm_output(selected_raw)
    all_selected_rids = []
    for rids in parsed['selected'].values():
        all_selected_rids.extend(rids)

    if not all_selected_rids:
        print("  ⚠️  LLM 未输出任何 SELECTED 条目")
        _result["status"] = "no_content"
        _result["success"] = True
        write_result()
        sys.exit(0)

    # ── RID 白名单校验 ───────────────────────────────────
    valid_rids = [r for r in all_selected_rids if r in rid_map]
    invalid_rids = [r for r in all_selected_rids if r not in rid_map]

    if invalid_rids:
        print(f"  🚫 RID 白名单校验失败：{len(invalid_rids)} 个编号不在候选池中")
        for r in invalid_rids:
            print(f"      - {r}")
        _result["warnings"].append({
            "code": "INVALID_RIDS",
            "message": f"{len(invalid_rids)} 个 RID 不在候选池中",
            "invalid_rids": invalid_rids,
        })
        _result["status"] = "failed"
        _result["error"] = {
            "code": "HALLUCINATED_RIDS",
            "message": f"LLM 输出了 {len(invalid_rids)} 个不存在的编号",
            "stage": "rid_validation",
            "retryable": True,
        }
        with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
            f.write(selected_raw)
        write_result()
        sys.exit(1)

    print(f"  ✅ RID 校验通过：{len(valid_rids)}/{len(all_selected_rids)} 个编号有效")

    # ── 组装最终日报（事实字段 100% 来自数据库）───────────
    final_report = assemble_report(parsed, rid_map, date_str)
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        f.write(final_report)
    print(f"  最终日报已保存: {len(final_report)} 字符 → {OUTPUT_PATH}")

    # ── DRY_RUN ──────────────────────────────────────────
    if DRY_RUN:
        print(f"  🔵 DRY_RUN：跳过实际发送（草稿已保存）")
        _result["metrics"]["send_ok"] = False      # 未真实发送
        _result["metrics"]["draft_ok"] = True     # 草稿已生成
        _result["status"] = "draft"
        _result["metrics"]["selected_count"] = len(valid_rids)
        # 用 valid_rids 对应的 record_id 回写
        selected_ids = [rid_map[r]['id'] for r in valid_rids if r in rid_map]
        if selected_ids:
            count = hermes_db.mark_hermes_used(selected_ids)
            _result["artifacts"]["selected_record_ids"] = selected_ids
            print(f"  SQLite 更新 {count} 条为 used")
        save_last_report([normalize_title(rid_map[r]['title']) for r in valid_rids])
        write_result()
        sys.exit(0)

    # ── 正式发送 ─────────────────────────────────────────
    send_ok = send_via_hermes(final_report)
    _result["metrics"]["send_ok"] = send_ok

    if send_ok:
        print("  发送成功 ✓")
        _result["metrics"]["selected_count"] = len(valid_rids)
        selected_ids = [rid_map[r]['id'] for r in valid_rids if r in rid_map]
        if selected_ids:
            count = hermes_db.mark_hermes_used(selected_ids)
            _result["artifacts"]["selected_record_ids"] = selected_ids
        save_last_report([normalize_title(rid_map[r]['title']) for r in valid_rids])

        if BITABLE_SYNC_ENABLED:
            try:
                sync_script = Path(__file__).resolve().parent / 'sync_to_bitable.py'
                result = subprocess.run(
                    ['python3', str(sync_script)],
                    capture_output=True, text=True, timeout=120,
                    env={**os.environ, 'TERM': 'dumb'}
                )
                if result.returncode == 0:
                    print(f"  观察面同步完成 ✅")
                    _result["metrics"]["bitable_sync_ok"] = True
                else:
                    print(f"  观察面同步异常: {result.stderr.strip()}")
            except Exception as e:
                print(f"  观察面同步跳过: {e}")

        _result["status"] = "ok" if not _result["warnings"] else "warning"
    else:
        print("  发送失败 ✗")
        _result["error"] = {"code": "SEND_FAILED", "message": "Hermes 发送失败", "stage": "send", "retryable": False}
        _result["status"] = "failed"

    write_result()

if __name__ == '__main__':
    main()
