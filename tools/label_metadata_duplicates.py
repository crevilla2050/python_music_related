#!/usr/bin/env python3
"""
label_metadata_duplicates.py

Detect likely duplicates using tag-level similarity heuristics.

IMPORTANT ARCHITECTURAL NOTE:
This script records *evidence only*. It does NOT decide:
- which file should be archived
- which file should be deleted
- which copy is ultimately canonical

Those decisions are deferred to later planning or UI review stages and
are applied exclusively by execute_actions.py.

Purpose and rationale:
- Some duplicate audio files are not byte-for-byte identical but
  represent the same track (different encodings, trims, or containers).
- This module uses lightweight fuzzy matching on artist/album/title
  (with an optional duration tolerance) to discover such cases.
- Matches from this stage are lower-confidence than SHA-256 exact
  matches but still valuable signals for downstream decision-making.

Design notes:
- The algorithm is intentionally simple and transparent (fuzzy ratios
  + small duration tolerance) so operators can understand and tune
  thresholds (`HIGH`, `MEDIUM`) without complex machine learning.
"""

import sqlite3
import os
from rapidfuzz import fuzz
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB", "music_consolidation.db")

HIGH = 0.90
MEDIUM = 0.75


def utcnow():
    """
    Return the current UTC time in ISO 8601 format.

    Why: Timestamps are stored with duplicate records so operators can
    trace when relationships were discovered. Using UTC avoids
    timezone-related confusion during audits.
    """
    return datetime.now(timezone.utc).isoformat()


def similarity(a, b):
    """
    Compute a normalized similarity score between two strings.

    Why: We use `rapidfuzz` to get a normalized fuzzy match ratio in the
    range [0.0, 1.0]. Returning 0 for missing values keeps the scoring
    logic straightforward and prevents exceptions when tags are null.
    """
    if not a or not b:
        return 0
    return fuzz.ratio(a, b) / 100.0


def main():
    """
    Main entry point for metadata-based duplicate detection.

    High-level algorithm:
    - Load candidate rows from the `files` table.
    - Compare each pair using fuzzy similarity on artist/album/title.
    - Prefer `album_artist` when available to improve matching for
      credited compilations.
    - Apply a short duration tolerance to boost confidence when the
      lengths are nearly identical.
    - If the combined score exceeds `MEDIUM`, insert a `duplicates`
      evidence row.

    NOTE:
    This script intentionally does NOT assign execution intent
    (status/action). It records metadata-based duplicate evidence only.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Enforce foreign-key constraints for safer inserts into `duplicates`.
    c.execute("PRAGMA foreign_keys = ON")

    # NOTE:
    # We intentionally do NOT restrict this to strictly 'pending' rows.
    # Metadata-based evidence may be relevant even after other labeling
    # stages (SHA-256, fingerprint) have run.
    c.execute("""
        SELECT
            id,
            artist,
            album_artist,
            album,
            title,
            duration,
            is_compilation
        FROM files
        WHERE status IS NULL OR status IN ('pending')
    """)
    rows = c.fetchall()

    seen = set()
    labeled = 0

    print(f"[INFO] Found {len(rows)} metadata comparison candidates")

    for i, r1 in enumerate(rows):
        for r2 in rows[i + 1:]:
            # Use a sorted tuple of IDs to ensure each pair is processed once.
            key = tuple(sorted((r1["id"], r2["id"])))
            if key in seen:
                continue

            # Safety: avoid matching across compilation boundaries.
            if r1["is_compilation"] != r2["is_compilation"]:
                continue

            # Prefer album_artist when available.
            a1 = r1["album_artist"] or r1["artist"]
            a2 = r2["album_artist"] or r2["artist"]

            artist_sim = similarity(a1, a2)
            title_sim = similarity(r1["title"], r2["title"])

            # Combine artist and title similarity with equal weight.
            score = (artist_sim * 0.5) + (title_sim * 0.5)

            # Apply small duration tolerance.
            if r1["duration"] and r2["duration"]:
                if abs(r1["duration"] - r2["duration"]) <= 3:
                    score += 0.05

            if score < MEDIUM:
                continue

            # Tiered confidence scoring.
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

            # NOTE:
            # The following UPDATE was intentionally disabled.
            #
            # Metadata-based matches are probabilistic and should not
            # assign execution intent. Decisions about archiving or
            # deletion are deferred to later planning or UI review steps.
            #
            # c.execute("""
            #     UPDATE files
            #     SET status='suspected_duplicate', action='archive'
            #     WHERE id=?
            # """, (dup,))

            seen.add(key)
            labeled += 1

    conn.commit()
    conn.close()

    print(f"[✓] Metadata duplicate relationships recorded: {labeled}")


if __name__ == "__main__":
    main()
