#!/usr/bin/env python3
"""
execute_actions.py

Authoritative execution phase for the consolidation pipeline.

Purpose and guarantees:
- This module performs the final filesystem operations indicated by
    the staging SQLite DB (`files` table). It is intentionally strict
    about its inputs: it NEVER infers file locations or recomputes
    metadata, and it only trusts the decisions already recorded in the
    database. This separation keeps analysis and execution responsibilities
    distinct and auditable.

Operational rules (design constraints):
- NEVER infer paths: use `recommended_path` written to the DB rather
    than guessing where a file should live.
- NEVER recompute metadata: do not read or change embedded tags during
    execution — all decisions come from the DB.
- ONLY trust the SQLite DB: the DB is the single source of truth for
    what actions to perform.
- ALL deletes are SOFT deletes: files are moved to a `trash_root`
    (default `to_trash`) so deletions are reversible and auditable.

The script is conservative and idempotent where possible: it commits
per-row so progress is durable and errors affect only the current row.
"""

import sqlite3
import argparse
import shutil
from pathlib import Path
from datetime import datetime, timezone

import os
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    raise SystemExit("[ERROR] MUSIC_DB not set in .env")

VALID_ACTIONS = {"move", "archive", "delete", "skip"}


# -------------------- helpers --------------------

def utcnow():
    """
    Return a UTC ISO-8601 timestamp string.

    Why: Execution events and DB updates are timestamped to provide a
    reliable audit trail. Using UTC prevents timezone-related sorting
    or comparison issues when multiple machines or operators inspect
    the records.
    """
    return datetime.now(timezone.utc).isoformat()

def log(msg):
    """
    Simple timestamped logger used for runtime output.

    Why: This lightweight logging makes it easy to correlate script
    output with DB `last_update` timestamps during post-run review.
    """
    print(f"[{utcnow()}] {msg}")

def connect_db(db_path):
    """
    Create a sqlite3 connection that returns row-like objects.

    Why: Using `sqlite3.Row` lets code address columns by name which
    improves readability and reduces index-based errors when updating
    rows during execution.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn

def ensure_parent(path: Path):
    """
    Ensure the parent directory for `path` exists.

    Why: File move/archiving operations create destination directories
    as needed. This helper centralizes that behavior and avoids race
    conditions when multiple moves target the same directory.
    """
    path.parent.mkdir(parents=True, exist_ok=True)


# -------------------- core execution --------------------

def execute(
    db_path,
    archive_root=None,
    trash_root="to_trash",
    dry_run=False,
    limit=None
):
    # Resolve and normalize filesystem roots.
    trash_root = Path(trash_root).resolve()
    if archive_root:
        archive_root = Path(archive_root).resolve()

    conn = connect_db(db_path)
    c = conn.cursor()

    # Load actionable rows determined by earlier pipeline stages. We
    # strictly filter by `action` values that the execution layer
    # knows how to perform and avoid rows already processed or in an
    # error state.
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

    # Track counts for human-friendly summary at the end of the run.
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
            # Validate source exists; if not, mark as error for review.
            if not src or not src.exists():
                raise RuntimeError(f"missing_source: {row['original_path']}")

            # ---------------- MOVE ----------------
            # Move files into their recommended canonical paths. The
            # `recommended_path` is considered authoritative because it
            # was produced by prior analysis stages.
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
            # Move files into an archive root supplied by the operator.
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
            # Soft-delete moves files to a `trash_root` directory rather
            # than permanently removing them, enabling recovery.
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
                # Mark as skipped; no filesystem operation is performed.
                log(f"[SKIP] {src}")
                c.execute("""
                    UPDATE files
                    SET status='skipped',
                        last_update=?
                    WHERE id=?
                """, (utcnow(), fid))
                summary["skipped"] += 1

            # Commit progress for each row so the state is durable even
            # if the script is interrupted later.
            conn.commit()

        except Exception as e:
            # On any error, mark the row `error` and include a note so
            # operators can triage what went wrong. We commit immediately
            # after updating the error state to avoid retry storms.
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
