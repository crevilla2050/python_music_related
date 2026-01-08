#!/usr/bin/env python3
"""
label_fingerprint_duplicates.py

Detect and label perceptual duplicates using stored Chromaprint
fingerprints.

This script records EVIDENCE ONLY.
No execution intent is applied here.
"""

import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB", "music_consolidation.db")

# ================= I18N MESSAGE KEYS =================

MSG_FP_SCAN_STARTED = "FINGERPRINT_DUP_SCAN_STARTED"
MSG_FP_CLUSTERS_FOUND = "FINGERPRINT_DUP_CLUSTERS_FOUND"
MSG_FP_DONE = "FINGERPRINT_DUP_RELATIONS_RECORDED"

# ====================================================


def utcnow():
    """
    Return current UTC timestamp as ISO 8601 string.
    """
    return datetime.now(timezone.utc).isoformat()


def main():
    """
    Label perceptual (fingerprint-based) duplicate relationships.
    """
    print(MSG_FP_SCAN_STARTED)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Ensure duplicate rows reference existing files.
    c.execute("PRAGMA foreign_keys = ON")

    # Discover fingerprint clusters (fingerprint present in >1 row).
    c.execute("""
        SELECT fingerprint
        FROM files
        WHERE fingerprint IS NOT NULL
        GROUP BY fingerprint
        HAVING COUNT(*) > 1
    """)
    fps = [r["fingerprint"] for r in c.fetchall()]

    print({
        "key": MSG_FP_CLUSTERS_FOUND,
        "params": {"count": len(fps)}
    })

    for fp in fps:
        # Fetch candidate rows for this fingerprint
        c.execute("""
            SELECT id, bitrate
            FROM files
            WHERE fingerprint = ?
        """, (fp,))
        rows = c.fetchall()

        if not rows:
            continue

        # Prefer highest bitrate as canonical representative
        rows = sorted(
            rows,
            key=lambda r: r["bitrate"] or 0,
            reverse=True
        )

        canonical = rows[0]["id"]

        for r in rows[1:]:
            dup_id = r["id"]

            # Record perceptual duplicate relationship
            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'fingerprint', 0.85, ?)
            """, (canonical, dup_id, utcnow()))

    conn.commit()
    conn.close()

    print(MSG_FP_DONE)


if __name__ == "__main__":
    main()
