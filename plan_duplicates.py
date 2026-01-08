#!/usr/bin/env python3
"""
plan_duplicates.py

Duplicate-planning stage for Pedro Organiza.

This module is responsible for *constructing execution plans* based on
duplicate evidence. It does NOT execute filesystem actions.

Pedro (execute_actions.py) is the worker.
This module is the thinker.
"""

import os
import sqlite3
import argparse
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


# -------------------- helpers --------------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()


def connect_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def lossless(ext):
    return ext.lower() in (".flac", ".wav", ".aiff", ".aif")


def preferred(a, b):
    """
    Decide which file is canonical.
    Returns (keep, archive)
    """
    if lossless(a["ext"]) != lossless(b["ext"]):
        return (a, b) if lossless(a["ext"]) else (b, a)

    if a["bitrate"] and b["bitrate"] and a["bitrate"] != b["bitrate"]:
        return (a, b) if a["bitrate"] > b["bitrate"] else (b, a)

    if a["size_bytes"] != b["size_bytes"]:
        return (a, b) if a["size_bytes"] > b["size_bytes"] else (b, a)

    return (a, b) if a["id"] < b["id"] else (b, a)


# -------------------- planner core --------------------

def plan_duplicates(db_path, apply=False, verbose=True):
    """
    Construct archive actions from duplicate evidence.

    Parameters:
        db_path (str): Path to SQLite database
        apply (bool): If True, write actions to DB. Otherwise dry-run.
        verbose (bool): Print proposed actions
    """
    conn = connect_db(db_path)
    c = conn.cursor()

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
        print(f"[PLAN] Evaluating {len(rows)} strong duplicate pairs")

    planned = 0

    for r in rows:
        # ---------- SAFETY FILTERS ----------

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

        exists = c.execute("""
            SELECT 1 FROM actions
            WHERE file_id = ?
              AND status = 'pending'
        """, (archive["id"],)).fetchone()

        if exists:
            continue

        if verbose:
            print(
                f"[PLAN] archive → {archive['path']} "
                f"(keep {keep['path']})"
            )

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
        print(
            "[DONE] "
            + ("Dry-run complete" if not apply else f"{planned} actions created")
        )


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(description="Plan duplicate archive actions")
    parser.add_argument("--db", help="Path to SQLite database")
    parser.add_argument("--apply", action="store_true", help="Write actions to DB")
    args = parser.parse_args()

    load_dotenv()

    db_path = args.db or os.getenv("MUSIC_DB")
    if not db_path:
        raise SystemExit("[ERROR] No database specified and MUSIC_DB not set")

    plan_duplicates(db_path=db_path, apply=args.apply)


if __name__ == "__main__":
    main()
# -------------------- end of file --------------------