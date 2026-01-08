#!/usr/bin/env python3
"""
backend/plan_duplicates.py

Duplicate-planning stage for Pedro Organiza.

This module analyses duplicate evidence recorded in the database and
constructs conservative execution plans (archive actions) that a
separate executor will apply. It intentionally does not perform any
filesystem mutations — its role is to decide which file to keep and
which to archive based on deterministic heuristics and safety checks.

Key design points:
- Be conservative: only propose actions when high-confidence duplicate
    evidence exists (the SQL query filters for strong signals).
- Respect album/artist boundaries to avoid cross-album mistakes.
- Prefer lossless and higher-bitrate files when choosing the canonical
    file to keep.
"""

import os
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

# ================= I18N MESSAGE KEYS =================

MSG_PLAN_START = "PLAN_DUPLICATES_START"
MSG_PLAN_PAIR_COUNT = "PLAN_DUPLICATES_PAIR_COUNT"
MSG_PLAN_ARCHIVE = "PLAN_DUPLICATES_ARCHIVE"
MSG_PLAN_DONE_DRY = "PLAN_DUPLICATES_DONE_DRY"
MSG_PLAN_DONE_APPLY = "PLAN_DUPLICATES_DONE_APPLY"
MSG_NO_DB = "PLAN_DUPLICATES_NO_DB"

# ====================================================


# -------------------- helpers --------------------

def utcnow():
    """Return current UTC time as ISO-8601 string for DB timestamps."""
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def lossless(ext):
    """Return True for extensions considered lossless.

    The function is intentionally simple and centralised so the set of
    lossless extensions can be adjusted in one place.
    """
    return ext.lower() in (".flac", ".wav", ".aiff", ".aif")


def preferred(a, b):
    """
    Decide which file is canonical.
    Returns (keep, archive)
    """
    # Prefer lossless formats over lossy ones
    if lossless(a["ext"]) != lossless(b["ext"]):
        return (a, b) if lossless(a["ext"]) else (b, a)

    # Next prefer higher bitrate when available (common for lossy files)
    if a["bitrate"] and b["bitrate"] and a["bitrate"] != b["bitrate"]:
        return (a, b) if a["bitrate"] > b["bitrate"] else (b, a)

    # Otherwise prefer larger filesize as a heuristic for quality
    if a["size_bytes"] != b["size_bytes"]:
        return (a, b) if a["size_bytes"] > b["size_bytes"] else (b, a)

    # Last-resort deterministic tie-breaker: lower id wins
    return (a, b) if a["id"] < b["id"] else (b, a)


# -------------------- planner core --------------------

def plan_duplicates(db_path, apply=False, verbose=True):
    """
    Construct archive actions from duplicate evidence.
    """
    conn = connect_db(db_path)
    c = conn.cursor()
    # Select high-confidence duplicate pairs where the reason is either
    # a direct content hash ('sha256') or a fingerprint match and the
    # system has recorded strong confidence (>= 0.9). We load file fields
    # required for safety checks and canonical selection.
    rows = c.execute("""
        SELECT
            d.id           AS dup_id,
            d.reason,
            d.confidence,

            f1.id          AS id1,
            f1.original_path AS path1,
            f1.album       AS album1,
            f1.album_artist AS artist1,
            f1.is_compilation AS comp1,
            f1.bitrate     AS bitrate1,
            f1.size_bytes AS size1,

            f2.id          AS id2,
            f2.original_path AS path2,
            f2.album       AS album2,
            f2.album_artist AS artist2,
            f2.is_compilation AS comp2,
            f2.bitrate     AS bitrate2,
            f2.size_bytes AS size2

        FROM duplicates d
        JOIN files f1 ON f1.id = d.file1_id
        JOIN files f2 ON f2.id = d.file2_id
        WHERE d.reason IN ('sha256','fingerprint')
          AND d.confidence >= 0.9
    """).fetchall()

    if verbose:
        print({
            "key": MSG_PLAN_PAIR_COUNT,
            "params": {"count": len(rows)}
        })

    planned = 0

    for r in rows:
        # ---------- SAFETY FILTERS ----------

        # Safety filters to avoid cross-album or compilation mistakes.
        # Only consider pairs from the same album by the same artist and
        # that are not marked as compilations.
        if r["album1"] != r["album2"]:
            continue

        if r["comp1"] or r["comp2"]:
            continue

        if r["artist1"] != r["artist2"]:
            continue

        # ---------- CANONICAL SELECTION ----------

        f1 = {
            "id": r["id1"],
            "path": r["path1"],
            "bitrate": r["bitrate1"],
            "size_bytes": r["size1"],
            "ext": Path(r["path1"]).suffix,
        }

        f2 = {
            "id": r["id2"],
            "path": r["path2"],
            "bitrate": r["bitrate2"],
            "size_bytes": r["size2"],
            "ext": Path(r["path2"]).suffix,
        }

        keep, archive = preferred(f1, f2)

        # ---------- EXISTING INTENT CHECK ----------
        # Do not plan duplicate work for files that already have a pending
        # action recorded — this keeps the planner idempotent across runs.
        exists = c.execute("""
            SELECT 1 FROM actions
            WHERE file_id = ?
              AND status = 'pending'
        """, (archive["id"],)).fetchone()

        if exists:
            continue

        # Informational output for dry-run / verbose mode
        if verbose:
            print({
                "key": MSG_PLAN_ARCHIVE,
                "params": {
                    "archive": archive["path"],
                    "keep": keep["path"]
                }
            })

        # Persist the planned archive action into the `actions` table when
        # `apply=True`. We record only minimal information: the target
        # file id, the action name and the source path. The executor will
        # perform the actual move later.
        if apply:
            c.execute("""
                INSERT INTO actions (
                    file_id,
                    action,
                    src_path,
                    created_at
                )
                VALUES (?, 'archive', ?, ?)
            """, (
                archive["id"],
                archive["path"],
                utcnow()
            ))
            planned += 1

    if apply:
        conn.commit()

    conn.close()

    if verbose:
        print({
            "key": MSG_PLAN_DONE_APPLY if apply else MSG_PLAN_DONE_DRY,
            "params": {"count": planned}
        })


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()

    db_path = args.db or os.getenv("MUSIC_DB")
    if not db_path:
        raise SystemExit(MSG_NO_DB)

    print({"key": MSG_PLAN_START})
    plan_duplicates(db_path=db_path, apply=args.apply)


if __name__ == "__main__":
    main()
# -------------------- end of file --------------------
