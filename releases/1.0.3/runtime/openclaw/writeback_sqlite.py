"""持久化 SQLite 写回工具 — 直接修改磁盘上的 db 文件"""
import sqlite3, json, sys, shutil, time, os

DB_PATH = os.environ.get('OPENCLAW_SQLITE_PATH', '')

if not DB_PATH:
    print("OPENCLAW_SQLITE_PATH is not configured")
    sys.exit(1)

BACKUP_PATH = DB_PATH + '.backup'

if len(sys.argv) < 4:
    print("Usage: writeback_sqlite.py '<json_record_ids>' '<doc_id>' '<doc_url>'")
    sys.exit(1)

record_ids = json.loads(sys.argv[1])
doc_id = sys.argv[2]
doc_url = sys.argv[3]

print(f"Records: {len(record_ids)} ids, doc={doc_id}", flush=True)

# Backup
shutil.copy2(DB_PATH, BACKUP_PATH)
print(f"Backup: {BACKUP_PATH}", flush=True)

conn = sqlite3.connect(DB_PATH, timeout=10.0)
cur = conn.cursor()

now_ms = int(time.time() * 1000)
placeholders = ','.join(['?'] * len(record_ids))
sql = f"""
    UPDATE materials
    SET openclaw_status = 'published',
        openclaw_selected_at = ?,
        openclaw_doc_id = ?,
        openclaw_doc_url = ?
    WHERE id IN ({placeholders})
"""
params = [now_ms, doc_id, doc_url] + record_ids
cur.execute(sql, params)
conn.commit()
print(f"Updated rows: {cur.rowcount}", flush=True)

# Verify
cur.execute(f"SELECT id, openclaw_status, openclaw_doc_id FROM materials WHERE id IN ({placeholders})", record_ids)
rows = cur.fetchall()
print(f"Verified: {json.dumps(rows)}", flush=True)

conn.close()
print("Done!", flush=True)
