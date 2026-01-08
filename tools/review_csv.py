#!/usr/bin/env python3
"""
review_csv.py

CSV-based human review layer for Pedro Organiza.

Responsibilities:
- Export file rows for offline review
- Import reviewed decisions as planned actions
- NEVER mutates filesystem
- NEVER applies actions
- Writes ONLY to `actions` table

Layer: Review / Planning
"""

import sqlite3
import csv
import argparse
import os
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

# ---------------- I18N KEYS ----------------

MSG_DB_NOT_SET = "ERROR_DB_NOT_SET"
MSG_EXPORT_DONE = "CSV_EXPORT_DONE"
MSG_IMPORT_DONE = "CSV_IMPORT_DONE"
MSG_INVALID_ACTION = "INVALID_ACTION_SKIPPED"
MSG_NO_ACTIONS = "NO_ACTIONS_IMPORTED"

# ------------------------------------------

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
CSV_PATH = "review.csv"

VALID_ACTIONS = {"move", "archive", "delete", "skip"}


# ---------------- helpers ----------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()


def connect_db():
    if not DB_PATH:
        raise SystemExit({"key": MSG_DB_NOT_SET})
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------- export ----------------

def export_csv(only_new=True):
    conn = connect_db()
    c = conn.cursor()

    query = """
        SELECT
            id,
            original_path,
            recommended_path
        FROM files
    """

    if only_new:
        query += " WHERE lifecycle_state='new'"

    rows = c.execute(query).fetchall()
    conn.close()

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "file_id",
            "current_path",
            "recommended_path",
            "proposed_action",
            "notes"
        ])

        for r in rows:
            writer.writerow([
                r["id"],
                r["original_path"],
                r["recommended_path"] or "",
                "move",
                ""
            ])

    print({
        "key": MSG_EXPORT_DONE,
        "params": {"path": CSV_PATH, "count": len(rows)}
    })


# ---------------- import ----------------

def import_csv():
    conn = connect_db()
    c = conn.cursor()

    created = 0

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            fid = int(row["file_id"])
            action = row["proposed_action"].strip().lower()
            notes = row.get("notes", "").strip()

            if action not in VALID_ACTIONS:
                print({
                    "key": MSG_INVALID_ACTION,
                    "params": {"id": fid, "action": action}
                })
                continue

            # Do not overwrite existing pending actions
            exists = c.execute("""
                SELECT 1 FROM actions
                WHERE file_id=? AND status='pending'
            """, (fid,)).fetchone()

            if exists:
                continue

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
                row["current_path"],
                row["recommended_path"] or None,
                utcnow()
            ))

            if notes:
                c.execute("""
                    UPDATE files
                    SET notes = COALESCE(notes,'') || ?
                    WHERE id=?
                """, (f" | csv:{notes}", fid))

            created += 1

    conn.commit()
    conn.close()

    print({
        "key": MSG_IMPORT_DONE,
        "params": {"count": created}
    })


# ---------------- CLI ----------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--export", action="store_true")
    parser.add_argument("--import", dest="do_import", action="store_true")
    parser.add_argument("--all", action="store_true",
                        help="Export all rows (default: only new)")

    args = parser.parse_args()

    if args.export:
        export_csv(only_new=not args.all)
    elif args.do_import:
        import_csv()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
