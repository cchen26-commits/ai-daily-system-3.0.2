#!/usr/bin/env python3
"""check_fetch — 抓取健康检查（SQLite 主判断）"""
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
sys.path.insert(0, str(RUNTIME_DIR))
import db as hermes_db
from config import BJT, CHECK_FETCH_HOURS, CHECK_FETCH_NEW_RECORDS_THRESHOLD

def main():
    stats = hermes_db.check_fetch_stats(hours=CHECK_FETCH_HOURS)
    now = datetime.now(BJT)
    new_recs = stats["new_records"]
    pending  = stats["pending"]
    batch    = stats["last_batch_id"]
    ts       = stats["last_fetch_ts"]
    ok       = new_recs >= CHECK_FETCH_NEW_RECORDS_THRESHOLD

    print(f"[{now.strftime('%Y-%m-%d %H:%M')}] check_fetch")
    print(f"  SQLite OK:        True")
    print(f"  最近 {stats['hours']}h 新增:    {new_recs} 条")
    print(f"  当前 pending:      {pending} 条")
    print(f"  最近批次:          {batch}")
    if ts:
        from_ts = datetime.fromtimestamp(ts / 1000, tz=BJT)
        print(f"  最近批次时间:      {from_ts.strftime('%Y-%m-%d %H:%M')}")
    print(f"  状态:              {'✅ OK' if ok else '⚠️ 异常'}")
    if not ok:
        print(f"  原因: 最近 {stats['hours']}h 新增 < {CHECK_FETCH_NEW_RECORDS_THRESHOLD}")

    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
