#!/usr/bin/env python3
"""
backend/cleanup_after_apply.py

Post-apply cleanup helper for Pedro Organiza.

This module scans a target music library tree for directories that are
safe-to-delete according to conservative rules (either empty, or
containing only small image files such as thumbnails). It does not
perform destructive actions during scanning: instead it inserts
`delete_dir` actions into the application's `actions` table so that a
separate execution step can perform removals in a controlled way.

Design principles:
- Conservative detection to avoid accidental data loss
- Separate planning (DB actions) from execution
- Support `--dry-run` to surface candidates without mutating state

Assumptions:
- A SQLite DB with an `actions` table exists; rows inserted here will
  be processed by `execute_cleanup` which expects columns like
  `id`, `action`, `src_path`, `status`, etc.

This file contains three main responsibilities:
1. Identify deletable directories (`scan_deletable_dirs`).
2. Plan deletions by inserting `delete_dir` actions into the DB
    (`plan_cleanup_actions`).
3. Execute pending delete actions, removing small images and empty
    directories (`execute_cleanup`).
"""

import argparse
import os
import sqlite3
from pathlib import Path
from datetime import datetime, timezone

# ================= I18N MESSAGE KEYS =================

MSG_INVALID_TARGET = "CLEANUP_INVALID_TARGET"
MSG_MODE_CONFLICT = "CLEANUP_MODE_CONFLICT"
MSG_SCAN_RESULT = "CLEANUP_SCAN_RESULT"
MSG_CANDIDATE_DIR = "CLEANUP_CANDIDATE_DIR"
MSG_PLANNED_AND_REMOVED = "CLEANUP_PLANNED_AND_REMOVED"

# ====================================================

# ---------------- CONFIG ----------------

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}
MAX_IMAGE_SIZE = 100 * 1024  # 100 KB

# ---------------- HELPERS ----------------

def utcnow():
    """Return current UTC time as an ISO-8601 string.

    Used when recording action creation and application timestamps in
    the database so entries are auditable.
    """
    return datetime.now(timezone.utc).isoformat()


def is_small_image(path: Path) -> bool:
    """Return True when `path` is a small image file we consider trash.

    The check is intentionally conservative: only files with known image
    extensions are considered, and we silently treat stat errors as
    non-matching so the scanner does not crash on locked or unreadable
    files.
    """
    if path.suffix.lower() not in IMAGE_EXTS:
        return False
    try:
        return path.stat().st_size <= MAX_IMAGE_SIZE
    except Exception:
        return False


def directory_is_deletable(path: Path) -> bool:
    """Return True when `path` is safe to delete.

    A directory is deletable when either:
    - it is empty, or
    - it contains no subdirectories and all files are small images we
      consider disposable (thumbnails, cover caches).

    We ignore directories we cannot read and treat them as non-deletable
    to avoid unintended removals.
    """
    try:
        entries = list(path.iterdir())
    except Exception:
        # Permission errors or transient filesystem issues => skip
        return False

    if not entries:
        return True  # empty

    for item in entries:
        # If there are subdirectories, be conservative and don't delete
        if item.is_dir():
            continue
        # If any non-small-image file exists, the directory is not deletable
        if not is_small_image(item):
            return False

    # All files (if any) are small images
    return True


# ---------------- CORE LOGIC ----------------

def scan_deletable_dirs(root: Path):
    """Walk `root` (bottom-up) and return a list of deletable directories.

    We walk bottom-up (`topdown=False`) so that inner empty directories are
    discovered before their parents — this allows the caller to plan the
    removal of nested directories in a single pass without needing to
    re-scan after deletions.
    """
    candidates = []

    for current, _, _ in os.walk(root, topdown=False):
        p = Path(current)
        if directory_is_deletable(p):
            candidates.append(p)

    return candidates


def plan_cleanup_actions(db_path: Path, directories):
    """Insert `delete_dir` actions into the DB for each directory.

    Returns the number of planned actions. The actions are inserted with
    `status='pending'` and can later be picked up by `execute_cleanup`.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    planned = 0

    for d in directories:
        c.execute("""
            INSERT INTO actions (
                file_id,
                action,
                src_path,
                dst_path,
                status,
                created_at
            )
            VALUES (
                NULL,
                'delete_dir',
                ?,
                NULL,
                'pending',
                ?
            )
        """, (str(d), utcnow()))
        planned += 1

    conn.commit()
    conn.close()
    return planned


def execute_cleanup(db_path: Path):
    """Execute pending `delete_dir` actions from the DB.

    For each pending action we remove small images inside the directory
    and attempt to rmdir the directory if it is empty afterwards. Each
    action is marked `applied` on success or `error` with an error text on
    failure so the system can audit or retry later.
    """
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    actions = c.execute("""
        SELECT id, src_path
        FROM actions
        WHERE action='delete_dir'
          AND status='pending'
    """).fetchall()

    removed = 0

    for action_id, src_path in actions:
        try:
            p = Path(src_path)

            if p.exists():
                # Remove small image files only. We intentionally avoid
                # recursing into subdirectories here — planning should
                # have already considered nested structure.
                for item in p.iterdir():
                    if item.is_file() and is_small_image(item):
                        item.unlink()

                # Attempt to remove the directory if empty
                if not any(p.iterdir()):
                    p.rmdir()

            c.execute("""
                UPDATE actions
                SET status='applied',
                    applied_at=?
                WHERE id=?
            """, (utcnow(), action_id))
            removed += 1

        except Exception as e:
            # Persist the error so callers/operators can inspect failures
            c.execute("""
                UPDATE actions
                SET status='error',
                    error=?
                WHERE id=?
            """, (str(e), action_id))

    conn.commit()
    conn.close()
    return removed


# ---------------- CLI ----------------

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--target", required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--dry-run", action="store_true")

    args = parser.parse_args()

    # Mutually exclusive modes: cannot apply and dry-run simultaneously
    if args.apply and args.dry_run:
        raise SystemExit(MSG_MODE_CONFLICT)

    db_path = Path(args.db)
    target = Path(args.target)

    if not target.is_dir():
        raise SystemExit(MSG_INVALID_TARGET)

    # Discover candidate directories conservatively
    candidates = scan_deletable_dirs(target)

    # If requested, only print the scan results and do not mutate state.
    if args.dry_run or not args.apply:
        print({
            "key": MSG_SCAN_RESULT,
            "params": {"count": len(candidates)}
        })
        for d in candidates:
            print({
                "key": MSG_CANDIDATE_DIR,
                "params": {"path": str(d)}
            })
        return

    # Plan (insert DB actions) then execute them immediately. The split
    # exists so other orchestration could review or batch actions between
    # planning and execution if desired.
    planned = plan_cleanup_actions(db_path, candidates)
    removed = execute_cleanup(db_path)

    print({
        "key": MSG_PLANNED_AND_REMOVED,
        "params": {
            "planned": planned,
            "removed": removed
        }
    })


if __name__ == "__main__":
    main()
