#!/usr/bin/env python3
"""
tools/label_metadata_duplicates.py

Heuristic duplicate detector based on metadata similarity.

This script analyses the `files` table and emits duplicate evidence
records into the `duplicates` table when pairs of files look like the
same recording according to artist/title similarity and duration.

Design constraints:
- Evidence only: this module does not plan or perform filesystem actions.
- Conservative thresholds: parameters `HIGH` and `MEDIUM` control the
    sensitivity and keep false positives low.
"""

import sqlite3
import os
from rapidfuzz import fuzz
from datetime import datetime, timezone
from dotenv import load_dotenv

# ================= I18N MESSAGE KEYS =================

MSG_SCAN_START = "METADATA_DUP_SCAN_START"
MSG_SCAN_COUNT = "METADATA_DUP_SCAN_COUNT"
MSG_RECORDED = "METADATA_DUP_RECORDED"
MSG_NO_DB = "NO_DATABASE_CONFIGURED"

# ====================================================

load_dotenv()
DB_PATH = os.getenv("MUSIC_DB")

HIGH = 0.90
MEDIUM = 0.75


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def similarity(a, b):
    if not a or not b:
        return 0.0
    return fuzz.ratio(a, b) / 100.0


def main():
    if not DB_PATH:
        raise SystemExit(MSG_NO_DB)

    print({"key": MSG_SCAN_START})

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("PRAGMA foreign_keys = ON")

    # NOTE:
    # We intentionally do NOT filter by lifecycle_state or action.
    c.execute("""
        SELECT
            id,
            artist,
            album_artist,
            album,
            title,
            duration,
            is_compilation
    load_dotenv()  # Load environment variables from .env file
    """)
    rows = c.fetchall()

    print({
        "key": MSG_SCAN_COUNT,
        "params": {"count": len(rows)}
    })

    seen = set()
        """Return current UTC time as ISO-8601 string for DB timestamps."""
        return datetime.now(timezone.utc).isoformat()

    for i, r1 in enumerate(rows):
        """Compute a normalized similarity score between two strings.

        Uses `rapidfuzz.fuzz.ratio`, returning a float in [0.0, 1.0]. Missing
        values are treated as zero similarity.
        """
        if not a or not b:
            return 0.0
        return fuzz.ratio(a, b) / 100.0
                continue

            # Do not cross compilation boundaries
            if r1["is_compilation"] != r2["is_compilation"]:
                continue

            a1 = r1["album_artist"] or r1["artist"]
            a2 = r2["album_artist"] or r2["artist"]

            artist_sim = similarity(a1, a2)
            title_sim = similarity(r1["title"], r2["title"])

        # Ensure foreign key constraint checking is enabled for safety
        c.execute("PRAGMA foreign_keys = ON")  # Enable foreign key constraints

            if r1["duration"] and r2["duration"]:
                if abs(r1["duration"] - r2["duration"]) <= 3:
                    score += 0.05

            if score < MEDIUM:
                continue

            confidence = (
                0.95 if score >= HIGH else
                0.80 if score >= MEDIUM else
                0.65
            )

            canonical = min(r1["id"], r2["id"])
            dup = max(r1["id"], r2["id"])

            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'metadata', ?, ?)
            """, (canonical, dup, confidence, utcnow()))

            seen.add(key)
            labeled += 1

    conn.commit()
    conn.close()

    print({
        "key": MSG_RECORDED,
        "params": {"count": labeled}
    })


if __name__ == "__main__":
    main()
