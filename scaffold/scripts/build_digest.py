from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone

from common import connect_db, hours_window, output_dir


def main() -> None:
    conn = connect_db()
    since = (datetime.now(timezone.utc) - timedelta(hours=hours_window())).isoformat()
    rows = conn.execute(
        """
        SELECT title, url, source, category, content_type, raw_excerpt, published_at, fetched_at
        FROM articles
        WHERE fetched_at >= ?
        ORDER BY COALESCE(published_at, fetched_at) DESC
        LIMIT 30
        """,
        (since,),
    ).fetchall()
    conn.close()

    grouped = defaultdict(list)
    for row in rows:
        grouped[row["category"]].append(row)

    lines = [
        f"# AI Daily Digest",
        "",
        f"- Window: last {hours_window()} hours",
        f"- Selected items: {len(rows)}",
        "",
    ]

    for category in ["今日热点", "技术突破", "企业动态", "工具/教程", "商业模式"]:
        items = grouped.get(category, [])
        if not items:
            continue
        lines.append(f"## {category}")
        lines.append("")
        for item in items[:6]:
            lines.append(f"- [{item['title']}]({item['url']})  [{item['source']}]")
            if item["raw_excerpt"]:
                lines.append(f"  - {item['raw_excerpt']}")
        lines.append("")

    output = output_dir()
    output.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = output / f"digest_{stamp}.md"
    path.write_text("\n".join(lines).strip() + "\n", encoding="utf-8")
    print(f"digest written: {path}")


if __name__ == "__main__":
    main()
