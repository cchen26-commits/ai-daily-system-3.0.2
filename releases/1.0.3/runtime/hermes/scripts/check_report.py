#!/usr/bin/env python3
"""check_report — 快报健康检查（SQLite 主判断）"""
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

RUNTIME_DIR = Path(__file__).resolve().parents[1] / "runtime"
sys.path.insert(0, str(RUNTIME_DIR))
import db as hermes_db
from config import BJT, CHECK_REPORT_HOURS

def main():
    stats = hermes_db.check_report_stats(hours=CHECK_REPORT_HOURS)
    now = datetime.now(BJT)
    recent_used = stats["recent_used"]
    pending     = stats["pending"]
    last_sent   = stats["last_sent_at"]

    # 判定逻辑
    if pending == 0:
        status = "OK (no pending)"
        ok = True
    elif recent_used > 0:
        status = "OK"
        ok = True
    else:
        status = "异常"
        ok = False

    print(f"[{now.strftime('%Y-%m-%d %H:%M')}] check_report")
    print(f"  SQLite OK:           True")
    print(f"  最近 {stats['hours']}h used:    {recent_used} 条")
    print(f"  当前 pending:         {pending} 条")
    if last_sent:
        from_ts = datetime.fromtimestamp(last_sent / 1000, tz=BJT)
        print(f"  最近发送时间:        {from_ts.strftime('%Y-%m-%d %H:%M')}")
    else:
        print(f"  最近发送时间:        无记录")
    print(f"  状态:               {'✅ ' + status if ok else '⚠️ ' + status}")

    sys.exit(0 if ok else 1)

if __name__ == "__main__":
    main()
