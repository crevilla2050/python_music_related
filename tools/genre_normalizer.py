#!/usr/bin/env python3
# genre_normalizer.py

import re
import sqlite3
from datetime import datetime, timezone

SPLIT_RE = re.compile(r"[;/,|]+")


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def normalize_token(token: str) -> str:
    """
    Normalize a genre token for comparison.
    Lowercase, strip non-alphanumerics.
    """
    return re.sub(r"\W+", "", token.lower())


def tokenize(raw: str):
    """
    Split a raw genre string into candidate tokens.
    """
    if not raw:
        return []
    return [t.strip() for t in SPLIT_RE.split(raw) if t.strip()]


def normalize_genres(db_path, dry_run=False):
    """
    Normalize genre tags into canonical genre relations.

    Returns:
        unmapped_tokens (set[str])
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    files = c.execute("""
        SELECT id, genre
        FROM files
        WHERE genre IS NOT NULL AND TRIM(genre) != ''
    """).fetchall()

    unmapped = set()
    applied = 0

    for f in files:
        tokens = tokenize(f["genre"])

        for raw_token in tokens:
            norm = normalize_token(raw_token)

            mapping = c.execute("""
                SELECT genre_id
                FROM genre_mappings
                WHERE normalized_token = ?
            """, (norm,)).fetchone()

            if mapping is None:
                unmapped.add(raw_token)
                continue

            if mapping["genre_id"] is None:
                # Explicitly ignored token
                continue

            if not dry_run:
                c.execute("""
                    INSERT OR IGNORE INTO file_genres (
                        file_id, genre_id, source, confidence, created_at
                    )
                    VALUES (?, ?, 'tag', 0.7, ?)
                """, (f["id"], mapping["genre_id"], utcnow()))

            applied += 1

    if not dry_run:
        conn.commit()

    conn.close()

    return {
        "applied": applied,
        "unmapped": sorted(unmapped),
    }
