from common import connect_db


SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    url TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,
    category TEXT NOT NULL,
    content_type TEXT NOT NULL,
    raw_excerpt TEXT,
    published_at TEXT,
    fetched_at TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_articles_fetched_at ON articles(fetched_at);
CREATE INDEX IF NOT EXISTS idx_articles_category ON articles(category);
"""


def main() -> None:
    conn = connect_db()
    conn.executescript(SCHEMA)
    conn.commit()
    conn.close()
    print("SQLite schema initialized.")


if __name__ == "__main__":
    main()
