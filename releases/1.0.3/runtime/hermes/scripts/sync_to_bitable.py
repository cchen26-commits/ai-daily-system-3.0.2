#!/usr/bin/env python3
"""
sync_to_bitable — SQLite → 飞书多维表同步脚本
用法:
  增量同步（今日）: python3 sync_to_bitable.py
  补同步（全量）:   python3 sync_to_bitable.py --all
  补同步（指定批次）: python3 sync_to_bitable.py --batch 20260420
  补同步（指定日期）: python3 sync_to_bitable.py --date 2026-04-19
  试运行（不改SQLite）: python3 sync_to_bitable.py --dry-run

同步口径:
  待同步: sync_status IS NULL OR sync_status != 'ok'
  已同步: sync_status = 'ok' AND sync_to_bitable_at IS NOT NULL
  判断依据: url 字段（映射到多维表"链接"列）+ 同步后写 sync_to_bitable_at
"""
import sys, os, json, time, subprocess, argparse
from datetime import datetime, timezone, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
sys.path.insert(0, str(RUNTIME_DIR))
import db as hermes_db
from config import BJT, BITABLE_BASE_TOKEN, BITABLE_TABLE_ID, BITABLE_SYNC_ENABLED

# ── 飞书多维表配置（从 config.py 读取，无硬编码）──────────
BASE_TOKEN=BITABLE_BASE_TOKEN
TABLE_ID   = BITABLE_TABLE_ID  # 从 HERMES_BITABLE_TABLE 环境变量读取，详见 config.py

# ── 启动时检查 ────────────────────────────────────────────
if not BASE_TOKEN or not TABLE_ID:
    print("[sync_to_bitable] ⚠️  观察面同步已禁用: HERMES_BITABLE_BASE_TOKEN 或 HERMES_BITABLE_TABLE 环境变量未配置")
    print("[sync_to_bitable]   主链路(fetch/generate)不受影响，观察面需配置后补同步")
    sys.exit(0)

if not BITABLE_SYNC_ENABLED:
    print("[sync_to_bitable] ℹ️  观察面同步已关闭 (HERMES_BITABLE_SYNC=false)")
    sys.exit(0)

# 多维表字段映射（主库字段 → 多维表列名）
FIELD_MAP = {
    "标题":       "title",
    "链接":       "url",          # 写入时转成 {link: url}
    "平台":       "platform",
    "发布时间":   "published_at", # ms → 飞书时间戳
    "分类":       "category",
    "AI相关度":   "ai_relevance",
    "来源层级":   "source_tier",
    "原文摘要":   "summary_raw",
    "去重指纹":   "fingerprint",
    "quality_score": "quality_score",
    "content_type":   "content_type",
}

# ── lark-cli API（统一用 bot token，读写权限最稳）──────────
def lark_api(method, path, data=None):
    cli = os.environ.get("LARK_CLI_BIN", "/tmp/lark-cli")
    args = [cli, "api", method, path, "--as", "bot", "--format", "json", "-q", "."]
    if data:
        args += ["--data", "-"]
        r = subprocess.run(
            args,
            input=json.dumps(data),
            capture_output=True, text=True, timeout=30,
            env={k: v for k, v in os.environ.items() if "proxy" not in k.lower()}
        )
    else:
        r = subprocess.run(args, capture_output=True, text=True, timeout=30)
    raw = r.stdout
    js  = raw.find("{")
    if js >= 0:
        return json.loads(raw[js:])
    return {}

# ── 多维表操作 ────────────────────────────────────────────
def get_bitable_existing_urls() -> set:
    """
    拉取多维表所有记录的链接字段，返回 url 集合（用于去重）。
    注意：全量拉取 5000+ 条记录较慢（约 60s）。
    增量同步（今日批次）时，多维表新增记录很少，可跳过预检查直接写入。
    """
    existing = set()
    page_token = ""
    while True:
        params = {"page_size": 500}
        if page_token:
            params["page_token"] = page_token
        resp = lark_api(
            "GET",
            f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{TABLE_ID}/records",
            params
        )
        items = resp.get("data", {}).get("items", [])
        for item in items:
            fields = item.get("fields", {})
            link_val = fields.get("链接", "")
            if isinstance(link_val, dict):
                link = link_val.get("link", "")
            elif isinstance(link_val, str):
                link = link_val
            else:
                link = ""
            if link:
                existing.add(link.strip())
        page_token = resp.get("data", {}).get("page_token", "")
        if not page_token:
            break
        time.sleep(0.3)
    return existing


def sync_batch(records: list, existing_urls: set) -> dict:
    """
    批量同步 records 到多维表。
    返回: {created: N, skipped: N, failed: N, errors: []}
    """
    to_create = []
    for r in records:
        url = (r.get("url") or "").strip()
        if url and url in existing_urls:
            to_create.append({**r, "_skipped": True})
        else:
            to_create.append({**r, "_skipped": False})

    new_records = [r for r in to_create if not r["_skipped"]]
    skipped = len(to_create) - len(new_records)

    if not new_records:
        return {"created": 0, "skipped": skipped, "failed": 0, "errors": []}

    # 批量写入（每批最多 500 条，但这里 10 条一批更安全）
    created = 0
    errors  = []
    for i in range(0, len(new_records), 10):
        batch = new_records[i:i+10]
        fields_list = []
        for r in batch:
            pub_at = r.get("published_at") or 0
            fields = {
                "标题":       (r.get("title") or "")[:500],
                "链接":       {"link": r.get("url", "")},
                "平台":       r.get("platform", ""),
                "发布时间":   pub_at,
                "分类":       r.get("category", ""),
                "AI相关度":   r.get("ai_relevance", ""),
                "来源层级":   r.get("source_tier", ""),
                "原文摘要":   (r.get("summary_raw") or "")[:1000],
                "去重指纹":   r.get("fingerprint", ""),
                "quality_score": r.get("quality_score") or 0,
                "content_type":   r.get("content_type") or "",
            }
            fields_list.append(fields)

        try:
            resp = lark_api(
                "POST",
                f"/open-apis/bitable/v1/apps/{BASE_TOKEN}/tables/{TABLE_ID}/records/batch_create",
                {"records": [{"fields": f} for f in fields_list]}
            )
            if resp.get("ok") or resp.get("code") == 0:
                created += len(resp.get("data", {}).get("records", []))
            else:
                err_msg = resp.get("msg", str(resp))
                errors.append(f"batch {(i//10)+1}: {err_msg}")
        except Exception as e:
            errors.append(f"batch {(i//10)+1} exception: {e}")
        time.sleep(0.4)

    return {"created": created, "skipped": skipped, "failed": len(errors), "errors": errors}


# ── 查询待同步记录 ─────────────────────────────────────────
def get_pending_records(mode: str = "today", batch_id: str = None, date_str: str = None) -> list:
    """
    mode='today':   今日本次抓取的批次（source_fetch_batch = 最新批次）
    mode='all':     全量待同步（sync_status != 'ok'）
    mode='batch':   指定批次 ID
    mode='date':    指定日期（格式 YYYY-MM-DD）
    """
    conn = hermes_db.get_conn()

    if mode == "today":
        # 取最新批次
        row = conn.execute("""
            SELECT source_fetch_batch, MAX(created_at) as ts
            FROM materials
            WHERE source_fetch_batch IS NOT NULL AND source_fetch_batch != ''
            GROUP BY source_fetch_batch
            ORDER BY ts DESC LIMIT 1
        """).fetchone()
        if not row or not row["source_fetch_batch"]:
            print("  无抓取批次记录")
            conn.close()
            return []
        batch = row["source_fetch_batch"]
        print(f"  批次: {batch}")
        rows = conn.execute("""
            SELECT id, title, url, platform, published_at, summary_raw,
                   category, ai_relevance, source_tier, fingerprint,
                   quality_score, content_type
            FROM materials
            WHERE source_fetch_batch = ?
              AND (sync_status IS NULL OR sync_status != 'ok')
        """, (batch,)).fetchall()

    elif mode == "all":
        rows = conn.execute("""
            SELECT id, title, url, platform, published_at, summary_raw,
                   category, ai_relevance, source_tier, fingerprint,
                   quality_score, content_type
            FROM materials
            WHERE sync_status IS NULL OR sync_status != 'ok'
            ORDER BY created_at ASC
            LIMIT 5000
        """).fetchall()

    elif mode == "batch":
        rows = conn.execute("""
            SELECT id, title, url, platform, published_at, summary_raw,
                   category, ai_relevance, source_tier, fingerprint,
                   quality_score, content_type
            FROM materials
            WHERE source_fetch_batch = ?
              AND (sync_status IS NULL OR sync_status != 'ok')
        """, (batch_id,)).fetchall()

    elif mode == "date":
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
        except:
            print(f"  日期格式错误: {date_str}，应为 YYYY-MM-DD")
            conn.close()
            return []
        start_ms = int(datetime.combine(d, datetime.min.time()).timestamp() * 1000)
        end_ms   = int(datetime.combine(d, datetime.max.time()).timestamp() * 1000)
        rows = conn.execute("""
            SELECT id, title, url, platform, published_at, summary_raw,
                   category, ai_relevance, source_tier, fingerprint,
                   quality_score, content_type
            FROM materials
            WHERE created_at >= ? AND created_at <= ?
              AND (sync_status IS NULL OR sync_status != 'ok')
            ORDER BY created_at ASC
        """, (start_ms, end_ms)).fetchall()

    conn.close()
    return [dict(row) for row in rows]


# ── 写回 SQLite 同步状态 ──────────────────────────────────
def update_sync_status(records: list, status: str, error_note: str = ""):
    if not records:
        return
    ids = [r["id"] for r in records if "id" in r]
    hermes_db.mark_synced(ids, status=status, error_note=error_note)


# ── 主同步流程 ─────────────────────────────────────────────
def do_sync(mode: str = "today", batch_id: str = None, date_str: str = None, dry_run: bool = False):
    ts = datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    print(f"[{ts}] sync_to_bitable 开始 (mode={mode})")

    # 1. 查待同步记录
    print("  查 SQLite 待同步记录...")
    pending = get_pending_records(mode=mode, batch_id=batch_id, date_str=date_str)
    print(f"  待同步: {len(pending)} 条")

    if not pending:
        print("  没有待同步记录 ✅")
        return {"scanned": 0, "created": 0, "skipped": 0, "failed": 0}

    # 2. 小批量（≤100条）跳过预拉取，直接写
    #    多维表 API 不支持 upsert，已在多维表的记录会重复写入（下次可手动去重）
    is_small_batch = len(pending) <= 100
    if is_small_batch:
        print(f"  小批量({len(pending)}条)，跳过预拉取直接写入")
        existing_urls = set()
    else:
        print("  拉取多维表已有链接（去重）...")
        existing_urls = get_bitable_existing_urls()
        print(f"  多维表已有: {len(existing_urls)} 条")

    # 3. 同步
    if dry_run:
        to_sync = [r for r in pending if (r.get("url") or "").strip() and r["url"].strip() not in existing_urls]
        to_skip = len(pending) - len(to_sync)
        print(f"  [DRY RUN] 将写入: {len(to_sync)} 条，跳过: {to_skip} 条")
        return {"scanned": len(pending), "created": len(to_sync), "skipped": to_skip, "failed": 0}

    result = sync_batch(pending, existing_urls)
    print(f"  写入: {result['created']} 条，跳过（已存在）: {result['skipped']} 条，失败: {result['failed']} 条")

    if result["errors"]:
        for e in result["errors"][:5]:
            print(f"    错误: {e}")

    # 4. 写回 SQLite（只更新成功写入的）
    if result["created"] > 0:
        created_records = [r for r in pending
                          if (r.get("url") or "").strip()
                          and r["url"].strip() not in existing_urls]
        # 取前 result['created'] 条（粗略，因为上面过滤了）
        # 更准确的方式：按顺序取前 N 条
        actual_created = created_records[:result["created"]] if result["created"] > 0 else []
        update_sync_status(actual_created, "ok")
        print(f"  SQLite sync_status → ok: {len(actual_created)} 条")

    # 5. 写回失败记录
    if result["failed"] > 0:
        err_note = "; ".join(result["errors"][:3])
        failed_records = [r for r in pending if (r.get("url") or "").strip() in existing_urls]
        # 失败记录难以精确对应，跳过（下次重跑会再次尝试）
        print(f"  失败记录未写 SQLite（下次重跑会再次尝试）")

    return {
        "scanned": len(pending),
        "created": result["created"],
        "skipped": result["skipped"],
        "failed":  result["failed"],
    }


# ── CLI 入口 ───────────────────────────────────────────────
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQLite → 飞书多维表同步")
    parser.add_argument("--all",      action="store_true", help="全量待同步记录")
    parser.add_argument("--batch",    help="指定批次 ID")
    parser.add_argument("--date",    help="指定日期 YYYY-MM-DD")
    parser.add_argument("--dry-run",  action="store_true", help="试运行，不写 SQLite")
    args = parser.parse_args()

    hermes_db.init_db()

    if args.all:
        mode = "all"
        batch_id, date_str = None, None
    elif args.batch:
        mode, batch_id, date_str = "batch", args.batch, None
    elif args.date:
        mode, batch_id, date_str = "date", None, args.date
    else:
        mode, batch_id, date_str = "today", None, None

    result = do_sync(mode=mode, batch_id=batch_id, date_str=date_str, dry_run=args.dry_run)

    ts = datetime.now(BJT).strftime("%Y-%m-%d %H:%M")
    print(f"\n[{ts}] 完成")
    print(f"  本次扫描: {result['scanned']} 条")
    print(f"  本次成功: {result['created']} 条")
    print(f"  本次跳过: {result['skipped']} 条")
    print(f"  本次失败: {result['failed']} 条")

    if result["failed"] > 0:
        print(f"  失败分类: API 批量写入层（需查看上方错误详情）")
