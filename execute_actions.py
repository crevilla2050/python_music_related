#!/usr/bin/env python3
"""
execute_actions.py

Pedro Organiza execution engine.

This module is a pure worker:
- Reads execution intent from `actions`
- Applies filesystem changes
- Updates execution state
- NEVER plans, infers, or decides
"""

import os
import sqlite3
import argparse
import shutil
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()


# -------------------- helpers --------------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()


def log(msg):
    print(f"[{utcnow()}] {msg}")


def connect_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_parent(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)


# -------------------- executor core --------------------

def execute_actions(
    db_path,
    archive_root=None,
    trash_root="to_trash",
    dry_run=True,
    limit=None,
):
    conn = connect_db(db_path)
    c = conn.cursor()

    archive_root = Path(archive_root).resolve() if archive_root else None
    trash_root = Path(trash_root).resolve()

    query = """
        SELECT
            a.id          AS action_id,
            a.file_id,
            a.action,
            a.src_path,
            a.dst_path,

            f.original_path
        FROM actions a
        JOIN files f ON f.id = a.file_id
        WHERE a.status = 'pending'
        ORDER BY a.id
    """

    if limit:
        query += f" LIMIT {int(limit)}"

    rows = c.execute(query).fetchall()
    log(f"Loaded {len(rows)} pending actions (dry_run={dry_run})")

    summary = {
        "move": 0,
        "archive": 0,
        "delete": 0,
        "skip": 0,
        "error": 0,
    }

    for r in rows:
        action_id = r["action_id"]
        action = r["action"]
        src = Path(r["src_path"])

        try:
            if not src.exists():
                raise RuntimeError(f"missing_source: {src}")

            # ---------------- MOVE ----------------
            if action == "move":
                if not r["dst_path"]:
                    raise RuntimeError("move_without_dst_path")

                dst = Path(r["dst_path"])
                ensure_parent(dst)

                if dst.exists():
                    raise RuntimeError(f"destination_exists: {dst}")

                log(f"[MOVE] {src} → {dst}")
                if not dry_run:
                    shutil.move(src, dst)

                if not dry_run:
                    c.execute("""
                        UPDATE files
                        SET original_path=?, last_update=?
                        WHERE id=?
                    """, (str(dst), utcnow(), r["file_id"]))

                summary["move"] += 1

            # ---------------- ARCHIVE ----------------
            elif action == "archive":
                if not archive_root:
                    raise RuntimeError("archive_root_not_provided")

                dst = archive_root / f"{r['file_id']}_{src.name}"
                ensure_parent(dst)

                if dst.exists():
                    raise RuntimeError(f"archive_destination_exists: {dst}")

                log(f"[ARCHIVE] {src} → {dst}")
                if not dry_run:
                    shutil.move(src, dst)

                if not dry_run:
                    c.execute("""
                        UPDATE files
                        SET original_path=?, last_update=?
                        WHERE id=?
                    """, (str(dst), utcnow(), r["file_id"]))

                summary["archive"] += 1

            # ---------------- DELETE (SOFT) ----------------
            elif action == "delete":
                dst = trash_root / f"{r['file_id']}_{src.name}"
                ensure_parent(dst)

                if dst.exists():
                    raise RuntimeError(f"trash_destination_exists: {dst}")

                log(f"[TRASH] {src} → {dst}")
                if not dry_run:
                    shutil.move(src, dst)

                if not dry_run:
                    c.execute("""
                        UPDATE files
                        SET original_path=?, last_update=?
                        WHERE id=?
                    """, (str(dst), utcnow(), r["file_id"]))

                summary["delete"] += 1

            # ---------------- SKIP ----------------
            elif action == "skip":
                log(f"[SKIP] {src}")
                summary["skip"] += 1

            else:
                raise RuntimeError(f"unknown_action: {action}")

            # ---------------- ACTION STATE ----------------
            if not dry_run:
                c.execute("""
                    UPDATE actions
                    SET status='applied', applied_at=?
                    WHERE id=?
                """, (utcnow(), action_id))

            conn.commit()

        except Exception as e:
            log(f"[ERROR] action_id={action_id}: {e}")

            if not dry_run:
                c.execute("""
                    UPDATE actions
                    SET status='error', error=?
                    WHERE id=?
                """, (str(e), action_id))
                conn.commit()

            summary["error"] += 1

    conn.close()

    log("Execution finished")
    for k, v in summary.items():
        log(f"  {k}: {v}")


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(description="Execute planned Pedro actions")
    parser.add_argument("--db", help="Path to SQLite database")
    parser.add_argument("--archive-root", help="Archive destination root")
    parser.add_argument("--trash-root", default="to_trash")
    parser.add_argument("--apply", action="store_true", help="Apply actions")
    parser.add_argument("--limit", type=int)
    args = parser.parse_args()

    db_path = args.db or os.getenv("MUSIC_DB")
    if not db_path:
        raise SystemExit("[ERROR] No database specified and MUSIC_DB not set")

    execute_actions(
        db_path=db_path,
        archive_root=args.archive_root,
        trash_root=args.trash_root,
        dry_run=not args.apply,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()
