#!/usr/bin/env python3
"""
resolve_duplicate_conflicts.py

Resolve duplicate evidence conflicts by selecting the strongest
duplicate reason per file.

Evidence priority:
    sha256 > fingerprint > metadata

IMPORTANT:
- This script DOES NOT assign actions
- This script DOES NOT modify lifecycle_state
- This script ONLY normalizes evidence
"""

import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

# ================= I18N MESSAGE KEYS =================

MSG_CONFLICT_RESOLVE_START = "DUPLICATE_CONFLICT_RESOLVE_START"
MSG_CONFLICT_RESOLVE_DONE = "DUPLICATE_CONFLICT_RESOLVE_DONE"
MSG_NO_DB = "NO_DATABASE_CONFIGURED"

# ====================================================

load_dotenv()
DB_PATH = os.getenv("MUSIC_DB")

PRIORITY = {
    "sha256": 3,
    "fingerprint": 2,
    "metadata": 1,
}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def main():
    if not DB_PATH:
        raise SystemExit(MSG_NO_DB)

    print({"key": MSG_CONFLICT_RESOLVE_START})

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Fetch all duplicate evidence
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

        if fid not in best or score > best[fid]["score"]:
            best[fid] = {
                "score": score,
                "reason": reason,
                "confidence": r["confidence"],
            }

    # Record resolved evidence (non-authoritative)
    for fid, data in best.items():
        note = (
            f"duplicate_resolved:"
            f"{data['reason']} "
            f"(confidence={data['confidence']})"
        )

        c.execute("""
            UPDATE files
            SET
                notes = COALESCE(notes, '') || ?
            WHERE id = ?
        """, (f" | {note}", fid))

    conn.commit()
    conn.close()

    print({
        "key": MSG_CONFLICT_RESOLVE_DONE,
        "params": {"count": len(best)}
    })


if __name__ == "__main__":
    main()
