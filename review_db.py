#!/usr/bin/env python3
"""
review_db.py

Interactive CLI reviewer for music_consolidation.db

- Review files one by one
- Assign action: move | skip | delete | archive | note | quit
- Edit proposed destination path
- Resume safely with --continue
- Designed for very large libraries (30k+ entries)

IMPORTANT:
This script ONLY updates the database.
No filesystem operations are performed.
"""

import sqlite3
import argparse
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = "music_consolidation.db"
DEFAULT_ACTION = "move"

ACTIONS = {
    "m": "move",
    "s": "skip",
    "d": "delete",
    "a": "archive",
    "n": "note",
    "p": "path",
    "q": "quit",
}

# -------------------- Helpers --------------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def pretty(value):
    return value if value not in (None, "", "None") else "-"

def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def print_header(fid):
    print("\n" + "=" * 80)
    print(f"File ID {fid}")
    print("=" * 80)

# -------------------- Query logic --------------------

def fetch_candidates(conn, resume=False, limit=None, only_duplicates=False):
    query = """
        SELECT
            f.id,
            f.original_path,
            f.artist,
            f.album,
            f.title,
            f.duration,
            f.bitrate,
            f.status,
            f.action,
            f.notes,
            f.recommended_path
        FROM files f
    """

    where = []
    params = []

    if resume:
        where.append("f.action = ?")
        params.append(DEFAULT_ACTION)

    if only_duplicates:
        where.append("""
            f.id IN (
                SELECT file1_id FROM duplicates
                UNION
                SELECT file2_id FROM duplicates
            )
        """)

    if where:
        query += " WHERE " + " AND ".join(where)

    query += " ORDER BY f.id"

    if limit:
        query += f" LIMIT {int(limit)}"

    cur = conn.execute(query, params)
    return cur.fetchall()

# -------------------- Review loop --------------------

def review_loop(rows):
    conn = connect_db()
    cur = conn.cursor()

    for row in rows:
        fid = row["id"]

        print_header(fid)
        print(f"Path       : {row['original_path']}")
        print(f"Artist     : {pretty(row['artist'])}")
        print(f"Album      : {pretty(row['album'])}")
        print(f"Title      : {pretty(row['title'])}")
        print(f"Duration   : {pretty(row['duration'])}")
        print(f"Bitrate    : {pretty(row['bitrate'])}")
        print(f"Status     : {pretty(row['status'])}")
        print(f"Action     : {pretty(row['action'])}")
        print(f"Notes      : {pretty(row['notes'])}")
        print(f"Proposed → : {pretty(row['recommended_path'])}")

        while True:
            print("\n[m]ove [s]kip [d]elete [a]rchive [p]ath [n]ote [q]uit")
            choice = input("> ").strip().lower()

            if choice not in ACTIONS:
                print("Invalid option.")
                continue

            action = ACTIONS[choice]

            # ---- Quit safely ----
            if action == "quit":
                print("Exiting review safely.")
                conn.commit()
                conn.close()
                return

            # ---- Edit note ----
            if action == "note":
                print("Enter note (empty = cancel):")
                note = input("> ").strip()
                if not note:
                    print("Note cancelled.")
                    continue

                cur.execute(
                    """
                    UPDATE files
                    SET notes = ?, last_update = ?
                    WHERE id = ?
                    """,
                    (note, utcnow(), fid),
                )
                conn.commit()
                print("Note saved.")
                continue

            # ---- Edit proposed path ----
            if action == "path":
                print("Enter new destination path (empty = cancel):")
                new_path = input("> ").strip()

                if not new_path:
                    print("Path edit cancelled.")
                    continue

                if not Path(new_path).suffix:
                    print("Path must include filename and extension.")
                    continue

                cur.execute(
                    """
                    UPDATE files
                    SET recommended_path = ?, last_update = ?
                    WHERE id = ?
                    """,
                    (new_path, utcnow(), fid),
                )
                conn.commit()
                print("Proposed path updated.")
                continue

            # ---- Final decision (move / skip / delete / archive) ----
            cur.execute(
                """
                UPDATE files
                SET action = ?, last_update = ?
                WHERE id = ?
                """,
                (action, utcnow(), fid),
            )
            conn.commit()
            break

    conn.close()
    print("\nReview complete.")

# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(description="Interactive DB reviewer")
    parser.add_argument(
        "--continue",
        dest="resume",
        action="store_true",
        help="Resume reviewing default-action rows only",
    )
    parser.add_argument("--limit", type=int, help="Limit number of rows reviewed")
    parser.add_argument(
        "--only-duplicates",
        action="store_true",
        help="Review only files involved in duplicates",
    )

    args = parser.parse_args()

    conn = connect_db()
    rows = fetch_candidates(
        conn,
        resume=args.resume,
        limit=args.limit,
        only_duplicates=args.only_duplicates,
    )
    conn.close()

    if not rows:
        print("No files to review.")
        return

    review_loop(rows)

if __name__ == "__main__":
    main()
