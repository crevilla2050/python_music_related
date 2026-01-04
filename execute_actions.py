#!/usr/bin/env python3
"""
execute_actions.py

Authoritative execution phase for music consolidation.

Rules:
- NEVER infer paths
- NEVER recompute metadata
- ONLY trust SQLite DB
- ALL deletes are SOFT deletes (to_trash/)
"""

import sqlite3
import argparse
import shutil
from pathlib import Path
from datetime import datetime, timezone

DEFAULT_DB = "music_consolidation.db"
VALID_ACTIONS = {"move", "archive", "delete", "skip"}


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


# -------------------- core execution --------------------

def execute(
    db_path,
    archive_root=None,
    trash_root="to_trash",
    dry_run=False,
    limit=None
):
    trash_root = Path(trash_root).resolve()
    if archive_root:
        archive_root = Path(archive_root).resolve()

    conn = connect_db(db_path)
    c = conn.cursor()

    query = """
        SELECT id, original_path, recommended_path, action, status, notes
        FROM files
        WHERE action IN ('move','archive','delete','skip')
          AND status NOT IN ('moved','deleted','skipped','error')
    """

    if limit:
        query += f" LIMIT {int(limit)}"

    rows = c.execute(query).fetchall()
    log(f"Loaded {len(rows)} actionable rows (dry_run={dry_run})")

    summary = {
        "moved": 0,
        "archived": 0,
        "deleted": 0,
        "skipped": 0,
        "errors": 0
    }

    for row in rows:
        fid = row["id"]
        action = row["action"]
        src = Path(row["original_path"]) if row["original_path"] else None
        dst = None

        try:
            if not src or not src.exists():
                raise RuntimeError(f"missing_source: {row['original_path']}")

            # ---------------- MOVE ----------------
            if action == "move":
                if not row["recommended_path"]:
                    raise RuntimeError("move_without_recommended_path")

                dst = Path(row["recommended_path"])
                ensure_parent(dst)

                log(f"[MOVE] {src} → {dst}")
                if not dry_run:
                    shutil.move(src, dst)

                c.execute("""
                    UPDATE files
                    SET status='moved',
                        original_path=?,
                        last_update=?,
                        notes=COALESCE(notes,'') || ' | moved'
                    WHERE id=?
                """, (str(dst), utcnow(), fid))
                summary["moved"] += 1

            # ---------------- ARCHIVE ----------------
            elif action == "archive":
                if not archive_root:
                    raise RuntimeError("archive_root_not_provided")

                dst = archive_root / src.name
                ensure_parent(dst)

                log(f"[ARCHIVE] {src} → {dst}")
                if not dry_run:
                    shutil.move(src, dst)

                c.execute("""
                    UPDATE files
                    SET status='moved',
                        original_path=?,
                        last_update=?,
                        notes=COALESCE(notes,'') || ' | archived'
                    WHERE id=?
                """, (str(dst), utcnow(), fid))
                summary["archived"] += 1

            # ---------------- SOFT DELETE ----------------
            elif action == "delete":
                dst = trash_root / src.name
                ensure_parent(dst)

                log(f"[TO_TRASH] {src} → {dst}")
                if not dry_run:
                    shutil.move(src, dst)

                c.execute("""
                    UPDATE files
                    SET status='deleted',
                        original_path=?,
                        last_update=?,
                        notes=COALESCE(notes,'') || ' | soft_deleted'
                    WHERE id=?
                """, (str(dst), utcnow(), fid))
                summary["deleted"] += 1

            # ---------------- SKIP ----------------
            elif action == "skip":
                log(f"[SKIP] {src}")
                c.execute("""
                    UPDATE files
                    SET status='skipped',
                        last_update=?
                    WHERE id=?
                """, (utcnow(), fid))
                summary["skipped"] += 1

            conn.commit()

        except Exception as e:
            log(f"[ERROR] ID {fid}: {e}")
            c.execute("""
                UPDATE files
                SET status='error',
                    last_update=?,
                    notes=COALESCE(notes,'') || ?
                WHERE id=?
            """, (utcnow(), f" | exec_error:{e}", fid))
            conn.commit()
            summary["errors"] += 1

    conn.close()

    log("Execution finished.")
    log("Summary:")
    for k, v in summary.items():
        log(f"  {k}: {v}")


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(description="Execute consolidation actions from SQLite DB")
    parser.add_argument("--db", default=DEFAULT_DB, help="Path to SQLite database")
    parser.add_argument("--archive-root", help="Root directory for archived files")
    parser.add_argument("--trash-root", default="to_trash", help="Soft-delete directory")
    parser.add_argument("--dry-run", action="store_true", help="Simulate actions only")
    parser.add_argument("--limit", type=int, help="Limit number of rows processed")

    args = parser.parse_args()

    execute(
        db_path=args.db,
        archive_root=args.archive_root,
        trash_root=args.trash_root,
        dry_run=args.dry_run,
        limit=args.limit
    )


if __name__ == "__main__":
    main()
