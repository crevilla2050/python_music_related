#!/usr/bin/env python3
"""
label_sha256_duplicates.py

Identify and record exact file duplicates using SHA-256 hashes.

Evidence only:
- No execution intent
- No filesystem changes
"""

import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# ================= I18N MESSAGE KEYS =================

MSG_SCAN_START = "SHA256_DUP_SCAN_START"
MSG_CLUSTERS_FOUND = "SHA256_DUP_CLUSTERS_FOUND"
MSG_RECORDED = "SHA256_DUP_RECORDED"
MSG_NO_DB = "NO_DATABASE_CONFIGURED"

# ====================================================

load_dotenv()
DB_PATH = os.getenv("MUSIC_DB")


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def main():
    if not DB_PATH:
        raise SystemExit(MSG_NO_DB)

    print({"key": MSG_SCAN_START})

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    c.execute("PRAGMA foreign_keys = ON")

    # Discover SHA-256 clusters
    c.execute("""
        SELECT sha256
        FROM files
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
    """)
    clusters = [r["sha256"] for r in c.fetchall()]

    print({
        "key": MSG_CLUSTERS_FOUND,
        "params": {"count": len(clusters)}
    })

    created = 0

    for sha in clusters:
        c.execute("""
            SELECT id
            FROM files
            WHERE sha256 = ?
            ORDER BY id
        """, (sha,))
        ids = [r["id"] for r in c.fetchall()]

        if not ids:
            continue

        canonical = ids[0]

        for dup_id in ids[1:]:
            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'sha256', 1.0, ?)
            """, (canonical, dup_id, utcnow()))
            created += 1

    conn.commit()
    conn.close()

    print({
        "key": MSG_RECORDED,
        "params": {"count": created}
    })


if __name__ == "__main__":
    main()
