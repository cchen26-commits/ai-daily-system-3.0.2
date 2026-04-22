from __future__ import annotations

from datetime import datetime, timedelta, timezone
from html import escape

from common import connect_db, hours_window, output_dir


HTML_HEAD = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>AI Daily Cards</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background:#f6f7f9; color:#111; margin:0; padding:24px; }
    .wrap { max-width: 980px; margin: 0 auto; }
    .grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap:16px; }
    .card { background:#fff; border:1px solid #e6e8eb; border-radius:14px; padding:16px; box-shadow:0 1px 2px rgba(0,0,0,.04); }
    .meta { font-size:12px; color:#666; margin-bottom:8px; }
    .title { font-size:16px; line-height:1.45; font-weight:600; margin:0 0 10px 0; }
    .excerpt { font-size:13px; line-height:1.6; color:#333; margin:0 0 12px 0; }
    a { color:#0f62fe; text-decoration:none; }
    h1 { margin:0 0 8px 0; }
    .sub { color:#666; margin-bottom:18px; }
  </style>
</head>
<body>
  <div class="wrap">
"""

HTML_TAIL = """
  </div>
</body>
</html>
"""


def main() -> None:
    conn = connect_db()
    since = (datetime.now(timezone.utc) - timedelta(hours=hours_window())).isoformat()
    rows = conn.execute(
        """
        SELECT title, url, source, category, raw_excerpt
        FROM articles
        WHERE fetched_at >= ?
        ORDER BY COALESCE(published_at, fetched_at) DESC
        LIMIT 12
        """,
        (since,),
    ).fetchall()
    conn.close()

    parts = [
        HTML_HEAD,
        "<h1>AI Daily Cards</h1>",
        f'<div class="sub">最近 {hours_window()} 小时，自动生成的最小可用卡片视图</div>',
        '<div class="grid">',
    ]

    for row in rows:
        parts.append(
            f"""
            <div class="card">
              <div class="meta">{escape(row['category'])} · {escape(row['source'])}</div>
              <p class="title"><a href="{escape(row['url'])}" target="_blank" rel="noreferrer">{escape(row['title'])}</a></p>
              <p class="excerpt">{escape(row['raw_excerpt'] or '')}</p>
            </div>
            """
        )

    parts.append("</div>")
    parts.append(HTML_TAIL)

    output = output_dir()
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output / f"cards_{stamp}.html"
    path.write_text("\n".join(parts), encoding="utf-8")
    print(f"cards written: {path}")


if __name__ == "__main__":
    main()
