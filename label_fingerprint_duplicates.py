#!/usr/bin/env python3
"""
label_fingerprint_duplicates.py

Detect and label perceptual duplicates using stored Chromaprint
fingerprints. This step complements byte-level SHA-256 duplicate
detection by identifying files that contain the same audio content
even if the files differ in encoding, container, or minor edits.

IMPORTANT ARCHITECTURAL NOTE:
This script records *evidence only*. It does NOT decide:
- which file should be moved
- which file should be archived
- which copy is ultimately canonical for execution

Those decisions are deferred to later planning or UI review stages and
are applied exclusively by execute_actions.py.

Behavior and rationale:
- Fingerprint matches are treated with lower confidence than exact
  SHA-256 matches.
- Relationships are recorded conservatively so downstream operators
  or automated planners can apply policy-aware decisions.
"""

import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB", "music_consolidation.db")


def utcnow():
    """
    Return current UTC timestamp as ISO 8601 string.

    Why: Timestamps are attached to duplicate records to indicate when
    the relationship was discovered. Using UTC avoids timezone issues
    when multiple tools or humans inspect the DB.
    """
    return datetime.now(timezone.utc).isoformat()


def main():
    """
    Main labeling routine.

    Steps:
    1. Enable foreign key enforcement to keep `duplicates` references
       valid.
    2. Find fingerprints that appear in multiple files (clusters).
    3. For each cluster, choose a canonical representative using a
       bitrate-based heuristic (highest bitrate first).
    4. Record perceptual duplicate relationships with conservative
       confidence scores.

    NOTE:
    Canonical selection here is *contextual* and used only to give
    duplicate relationships a stable direction. It does not imply
    execution intent.
    """
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

    print(f"[INFO] Found {len(fps)} fingerprint duplicate clusters")

    for fp in fps:
        # Fetch candidate rows for this fingerprint. We pull `bitrate`
        # because it is used as a simple quality heuristic when
        # choosing a canonical representative.
        c.execute("""
            SELECT id, bitrate
            FROM files
            WHERE fingerprint = ?
        """, (fp,))
        rows = c.fetchall()

        if not rows:
            continue

        # Prefer the highest-bitrate file as the canonical representative.
        # This heuristic is deterministic and favors higher-quality
        # encodings, but it does NOT imply execution intent.
        rows = sorted(
            rows,
            key=lambda r: r["bitrate"] or 0,
            reverse=True
        )

        canonical = rows[0]["id"]

        # NOTE:
        # The following UPDATE was intentionally disabled.
        #
        # Assigning status/action here would prematurely convert
        # probabilistic evidence into execution intent. Canonical
        # selection is used only to orient duplicate relationships.
        #
        # c.execute("""
        #     UPDATE files
        #     SET status='unique', action='move'
        #     WHERE id=?
        # """, (canonical,))

        for r in rows[1:]:
            dup_id = r["id"]

            # Record the perceptual duplicate relationship. Confidence
            # is intentionally < 1.0 to reflect the fuzzy nature of
            # fingerprint matching.
            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'fingerprint', 0.85, ?)
            """, (canonical, dup_id, utcnow()))

            # NOTE:
            # This UPDATE was intentionally disabled for the same reason
            # as the canonical update above. This script records
            # perceptual duplicate evidence only.
            #
            # c.execute("""
            #     UPDATE files
            #     SET status='duplicate', action='archive'
            #     WHERE id=?
            # """, (dup_id,))

    conn.commit()
    conn.close()

    print("[✓] Fingerprint duplicate relationships recorded")


if __name__ == "__main__":
    main()
