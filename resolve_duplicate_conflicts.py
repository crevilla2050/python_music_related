#!/usr/bin/env python3
"""
resolve_duplicate_conflicts.py

Ensures strongest duplicate evidence wins:
sha256 > fingerprint > metadata
"""

import sqlite3
import os
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("MUSIC_DB")

PRIORITY = {
    "sha256": 3,
    "fingerprint": 2,
    "metadata": 1
}

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Gather all duplicate relations
    c.execute("""
        SELECT
            file2_id,
            reason,
            confidence
        FROM duplicates
    """)
    rows = c.fetchall()

    best = {}

    for r in rows:
        fid = r["file2_id"]
        reason = r["reason"]

        if reason not in PRIORITY:
            continue

        score = PRIORITY[reason]

        if fid not in best or score > best[fid][0]:
            best[fid] = (score, reason, r["confidence"])

    # Apply normalization
    for fid, (_, reason, confidence) in best.items():
        c.execute("""
            UPDATE files
            SET
                status='duplicate',
                action='archive',
                notes=?
            WHERE id=?
        """, (f"{reason}_match (confidence={confidence})", fid))

    conn.commit()
    conn.close()

    print(f"[✓] Conflict resolution applied to {len(best)} files")

if __name__ == "__main__":
    main()
