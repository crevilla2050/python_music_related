#!/usr/bin/env python3
"""
review_db.py

Interactive CLI reviewer for Pedro Organiza.

Layer: Review / Planning

Responsibilities:
- Show file metadata + duplicate evidence
- Allow user to PLAN actions (not execute)
- Create or update rows in `actions`
- Append notes safely
- NEVER touch filesystem
"""

import sqlite3
import argparse
from datetime import datetime, timezone
from pathlib import Path
import os
from dotenv import load_dotenv

# ---------------- I18N KEYS ----------------

MSG_REVIEW_START = "REVIEW_START"
MSG_NO_FILES = "NO_FILES_TO_REVIEW"
MSG_EXITING = "REVIEW_EXITING"
MSG_INVALID_OPTION = "INVALID_OPTION"
MSG_ACTION_PLANNED = "ACTION_PLANNED"
MSG_NOTE_SAVED = "NOTE_SAVED"
MSG_PATH_UPDATED = "PATH_UPDATED"

# ------------------------------------------

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    raise SystemExit({"key": "ERROR_DB_NOT_SET"})

ACTIONS = {
    "m": "move",
    "s": "skip",
    "d": "delete",
    "a": "archive",
    "n": "note",
    "p": "path",
    "q": "quit",
}

# ---------------- helpers ----------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def pretty(v):
    return v if v not in (None, "", "None") else "-"

def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def header(fid):
    print("\n" + "=" * 80)
    print(f"FILE {fid}")
    print("=" * 80)

# ---------------- queries ----------------

def fetch_candidates(limit=None, only_duplicates=False):
    conn = connect_db()
    c = conn.cursor()

    query = """
        SELECT
            f.id,
            f.original_path,
            f.artist,
            f.album,
            f.title,
            f.duration,
            f.bitrate,
            f.lifecycle_state,
            f.notes,
            f.recommended_path,
            d.reason AS dup_reason,
            d.confidence AS dup_confidence
        FROM files f
        LEFT JOIN duplicates d
          ON f.id = d.file2_id
        WHERE f.lifecycle_state IN ('new','reviewing')
          AND NOT EXISTS (
            SELECT 1 FROM actions
            WHERE actions.file_id = f.id
              AND actions.status = 'pending'
          )
        ORDER BY f.id
    """

    if limit:
        query += f" LIMIT {int(limit)}"

    rows = c.execute(query).fetchall()
    conn.close()
    return rows

# ---------------- review loop ----------------

def review_loop(rows):
    conn = connect_db()
    c = conn.cursor()

    print({"key": MSG_REVIEW_START, "params": {"count": len(rows)}})

    for row in rows:
        fid = row["id"]
        header(fid)

        print(f"Path       : {row['original_path']}")
        print(f"Artist     : {pretty(row['artist'])}")
        print(f"Album      : {pretty(row['album'])}")
        print(f"Title      : {pretty(row['title'])}")
        print(f"Duration   : {pretty(row['duration'])}")
        print(f"Bitrate    : {pretty(row['bitrate'])}")
        print(f"Lifecycle  : {row['lifecycle_state']}")
        print(f"Notes      : {pretty(row['notes'])}")
        print(f"Proposed → : {pretty(row['recommended_path'])}")

        if row["dup_reason"]:
            print(f"Duplicate  : {row['dup_reason']} ({row['dup_confidence']})")

        while True:
            print("\n[m]ove [s]kip [d]elete [a]rchive [p]ath [n]ote [q]uit")
            choice = input("> ").strip().lower()

            if choice not in ACTIONS:
                print({"key": MSG_INVALID_OPTION})
                continue

            action = ACTIONS[choice]

            if action == "quit":
                print({"key": MSG_EXITING})
                conn.commit()
                conn.close()
                return

            if action == "note":
                note = input("> ").strip()
                if note:
                    c.execute("""
                        UPDATE files
                        SET notes = COALESCE(notes,'') || ?
                        WHERE id=?
                    """, (f" | {note}", fid))
                    conn.commit()
                    print({"key": MSG_NOTE_SAVED})
                continue

            if action == "path":
                new_path = input("> ").strip()
                if new_path and Path(new_path).suffix:
                    c.execute("""
                        UPDATE files
                        SET recommended_path = ?
                        WHERE id=?
                    """, (new_path, fid))
                    conn.commit()
                    print({"key": MSG_PATH_UPDATED})
                continue

            # ---- plan action ----
            c.execute("""
                INSERT INTO actions (
                    file_id,
                    action,
                    src_path,
                    dst_path,
                    status,
                    created_at
                )
                VALUES (?, ?, ?, ?, 'pending', ?)
            """, (
                fid,
                action,
                row["original_path"],
                row["recommended_path"],
                utcnow()
            ))

            c.execute("""
                UPDATE files
                SET lifecycle_state='reviewing'
                WHERE id=?
            """, (fid,))

            conn.commit()
            print({"key": MSG_ACTION_PLANNED, "params": {"action": action}})
            break

    conn.close()
    print({"key": "REVIEW_COMPLETE"})

# ---------------- CLI ----------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--limit", type=int)
    parser.add_argument("--only-duplicates", action="store_true")
    args = parser.parse_args()

    rows = fetch_candidates(
        limit=args.limit,
        only_duplicates=args.only_duplicates
    )

    if not rows:
        print({"key": MSG_NO_FILES})
        return

    review_loop(rows)

if __name__ == "__main__":
    main()
