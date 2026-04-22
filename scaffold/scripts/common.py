from __future__ import annotations

import os
import re
import sqlite3
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html import unescape
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB = ROOT / "data" / "ai_daily.db"
DEFAULT_SOURCES = ROOT / "config" / "sources.example.yaml"
DEFAULT_OUTPUT = ROOT / "output"


def env_path(name: str, fallback: Path) -> Path:
    value = os.environ.get(name)
    return Path(value).expanduser() if value else fallback


def db_path() -> Path:
    return env_path("AI_DAILY_DB", DEFAULT_DB)


def sources_path() -> Path:
    return env_path("AI_DAILY_SOURCES", DEFAULT_SOURCES)


def output_dir() -> Path:
    return env_path("AI_DAILY_OUTPUT", DEFAULT_OUTPUT)


def hours_window() -> int:
    raw = os.environ.get("AI_DAILY_HOURS", "24")
    try:
        return int(raw)
    except ValueError:
        return 24


def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)


def now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def connect_db() -> sqlite3.Connection:
    path = db_path()
    ensure_parent(path)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    return conn


def load_sources() -> list[dict[str, Any]]:
    import yaml

    with open(sources_path(), "r", encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data.get("sources", [])


def clean_text(text: str | None) -> str:
    if not text:
        return ""
    text = unescape(text)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def parse_datetime(value: Any) -> str | None:
    if not value:
        return None
    if isinstance(value, str):
        try:
            return parsedate_to_datetime(value).astimezone(timezone.utc).isoformat()
        except Exception:
            return None
    if hasattr(value, "tm_year"):
        try:
            return datetime(*value[:6], tzinfo=timezone.utc).isoformat()
        except Exception:
            return None
    return None
