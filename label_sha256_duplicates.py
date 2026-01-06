#!/usr/bin/env python3
"""
label_sha256_duplicates.py

Identify and label exact file duplicates using SHA-256 content
hashes. This script is part of the consolidation pipeline's duplicate
resolution stage and performs one focused action:

- Insert rows into the `duplicates` table to record exact-match
  relationships between files that share identical SHA-256 hashes.

IMPORTANT ARCHITECTURAL NOTE:
This script intentionally records *evidence only*. It does NOT decide:
- which file should be kept
- which file should be moved
- which file should be archived or deleted

Those decisions belong to later planning / UI review stages and are
applied exclusively by execute_actions.py.

Why a dedicated script:
- SHA-256 exact-duplicate detection is cheap and deterministic.
- Separating evidence collection from decision-making keeps the system
  auditable, reversible, and safe to evolve.

The script is intentionally idempotent: it uses INSERT OR IGNORE and
deterministic ordering so multiple executions do not create duplicate
database rows or inconsistent state.
"""

import sqlite3
import os
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB", "music_consolidation.db")


def utcnow():
    """
    Return the current UTC time as an ISO 8601 string.

    Why: Timestamps are stored alongside duplicate records so operators
    can see when relationships were discovered. Using UTC avoids
    timezone ambiguity when comparing across tools or environments.
    """
    return datetime.now(timezone.utc).isoformat()


def main():
    """
    Traverse every SHA-256 cluster and persist exact-duplicate evidence.

    Important details:
    - Foreign-key enforcement is enabled so duplicate relationships
      cannot reference missing files.
    - Clusters are discovered by grouping files with identical
      non-null `sha256` values and selecting groups with more than one
      member.
    - A deterministic canonical representative is chosen as the lowest
      `id` in each cluster. This choice is *recorded implicitly* via
      duplicate relationships but does NOT imply execution intent.
    - Each relationship is recorded with:
        - reason: 'sha256'
        - confidence: 1.0 (exact content match)

    No filesystem actions or execution directives are assigned here.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Enforce foreign-key constraints to ensure duplicate rows cannot
    # reference missing files; this makes later cleanup and queries
    # more reliable.
    c.execute("PRAGMA foreign_keys = ON")

    # Find SHA-256 values that appear in more than one file row.
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
        # Get all file IDs that share this SHA-256 hash. Ordering by
        # `id` ensures deterministic behavior across runs.
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

        # NOTE:
        # The following UPDATE was intentionally disabled.
        #
        # Assigning status/action here would prematurely turn evidence
        # into execution intent. Canonical selection is recorded
        # implicitly via duplicate relationships and resolved later
        # by planning or UI-driven steps.
        #
        # c.execute("""
        #     UPDATE files
        #     SET status='unique', action='move'
        #     WHERE id=?
        # """, (canonical,))

        for dup_id in ids[1:]:
            # Record the duplicate relationship (canonical -> duplicate)
            # with maximum confidence since SHA-256 is an exact content
            # match. INSERT OR IGNORE keeps the operation idempotent.
            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'sha256', 1.0, ?)
            """, (canonical, dup_id, utcnow()))

            # NOTE:
            # This UPDATE was intentionally disabled for the same reason
            # as the canonical update above. This script records evidence
            # only; it does not decide archival or deletion.
            #
            # c.execute("""
            #     UPDATE files
            #     SET status='duplicate', action='archive'
            #     WHERE id=?
            # """, (dup_id,))

            created += 1

    conn.commit()
    conn.close()

    print(f"[✓] SHA-256 duplicate relationships recorded ({created})")


if __name__ == "__main__":
    main()
