#!/usr/bin/env python3
"""
label_sha256_duplicates.py

Label exact duplicates based on SHA-256.
Populates `duplicates` table and updates files table.
"""

import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB", "music_consolidation.db")


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # NEW: enforce FK integrity
    c.execute("PRAGMA foreign_keys = ON")

    c.execute("""
        SELECT sha256
        FROM files
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
    """)
    clusters = [r["sha256"] for r in c.fetchall()]

    print(f"[INFO] Found {len(clusters)} SHA-256 duplicate clusters")

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

        # Canonical is always unique + move
        c.execute("""
            UPDATE files
            SET status='unique', action='move'
            WHERE id=?
        """, (canonical,))

        for dup_id in ids[1:]:
            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'sha256', 1.0, ?)
            """, (canonical, dup_id, utcnow()))

            c.execute("""
                UPDATE files
                SET status='duplicate', action='archive'
                WHERE id=?
            """, (dup_id,))

            created += 1

    conn.commit()
    conn.close()

    print(f"[✓] SHA-256 duplicates labeled successfully ({created})")


if __name__ == "__main__":
    main()
