from __future__ import annotations

import feedparser

from common import clean_text, connect_db, load_sources, now_iso, parse_datetime


def extract_excerpt(entry: dict) -> str:
    if "summary" in entry:
        text = clean_text(entry.get("summary"))
        return text[:220]
    if "description" in entry:
        text = clean_text(entry.get("description"))
        return text[:220]
    if "content" in entry and entry["content"]:
        text = clean_text(entry["content"][0].get("value", ""))
        return text[:220]
    return ""


def main() -> None:
    conn = connect_db()
    inserted = 0
    seen = 0

    for source in load_sources():
        feed = feedparser.parse(source["url"])
        fetched_at = now_iso()
        for entry in feed.entries:
            title = clean_text(entry.get("title"))
            url = entry.get("link", "").strip()
            if not title or not url:
                continue

            published_at = (
                parse_datetime(entry.get("published"))
                or parse_datetime(entry.get("updated"))
                or parse_datetime(entry.get("published_parsed"))
                or parse_datetime(entry.get("updated_parsed"))
            )
            raw_excerpt = extract_excerpt(entry)

            cur = conn.execute(
                """
                INSERT OR IGNORE INTO articles
                (title, url, source, category, content_type, raw_excerpt, published_at, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    title,
                    url,
                    source["name"],
                    source.get("category", "今日热点"),
                    source.get("content_type", "news"),
                    raw_excerpt,
                    published_at,
                    fetched_at,
                ),
            )
            if cur.rowcount:
                inserted += 1
            else:
                seen += 1

    conn.commit()
    conn.close()
    print(f"fetch completed: inserted={inserted}, skipped_existing={seen}")


if __name__ == "__main__":
    main()
