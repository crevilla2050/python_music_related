#!/usr/bin/env python3
"""
label_fingerprint_duplicates.py

Label perceptual duplicates based on Chromaprint fingerprint.
Lower confidence than SHA-256.
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

    # NEW
    c.execute("PRAGMA foreign_keys = ON")

    c.execute("""
        SELECT fingerprint
        FROM files
        WHERE fingerprint IS NOT NULL
        GROUP BY fingerprint
        HAVING COUNT(*) > 1
    """)
    fps = [r["fingerprint"] for r in c.fetchall()]

    print(f"[INFO] Found {len(fps)} fingerprint duplicate clusters")

    for fp in fps:
        c.execute("""
            SELECT id, bitrate
            FROM files
            WHERE fingerprint = ?
        """, (fp,))
        rows = c.fetchall()

        if not rows:
            continue

        rows = sorted(
            rows,
            key=lambda r: r["bitrate"] or 0,
            reverse=True
        )

        canonical = rows[0]["id"]

        c.execute("""
            UPDATE files
            SET status='unique', action='move'
            WHERE id=?
        """, (canonical,))

        for r in rows[1:]:
            dup_id = r["id"]

            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'fingerprint', 0.85, ?)
            """, (canonical, dup_id, utcnow()))

            c.execute("""
                UPDATE files
                SET status='duplicate', action='archive'
                WHERE id=?
            """, (dup_id,))

    conn.commit()
    conn.close()

    print("[✓] Fingerprint duplicates resolved")


if __name__ == "__main__":
    main()
