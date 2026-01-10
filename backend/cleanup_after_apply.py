#!/usr/bin/env python3
"""
backend/cleanup_after_apply.py

Post-apply cleanup helper for Pedro Organiza.

Scans a target library tree for directories safe to delete
(empty or containing only small disposable images).

This module PLANS deletions by inserting `delete_dir` actions
and optionally EXECUTES them in a controlled manner.

Layer: Planning + Optional Execution
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
MSG_SKIPPED_DIRS = "CLEANUP_SKIPPED_DIRS"

# ====================================================

# ---------------- CONFIG ----------------

IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}
MAX_IMAGE_SIZE = 100 * 1024  # 100 KB

# ---------------- HELPERS ----------------

def utcnow():
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


def is_small_image(path: Path) -> bool:
    """Return True if path is a small disposable image file."""
    if path.suffix.lower() not in IMAGE_EXTS:
        return False
    try:
        return path.stat().st_size <= MAX_IMAGE_SIZE
    except Exception:
        return False


def directory_is_deletable(path: Path) -> bool:
    """Return True if directory is empty or contains only small images."""
    try:
        entries = list(path.iterdir())
    except Exception:
        return False

    if not entries:
        return True

    for item in entries:
        if item.is_dir():
            return False
        if not is_small_image(item):
            return False

    return True


# ---------------- CORE LOGIC ----------------

def scan_deletable_dirs(root: Path):
    """Bottom-up scan returning (candidates, skipped_count)."""
    candidates = []
    skipped = 0

    for current, _, _ in os.walk(root, topdown=False):
        p = Path(current)
        if directory_is_deletable(p):
            candidates.append(p)
        else:
            skipped += 1

    return candidates, skipped


def plan_cleanup_actions(db_path: Path, directories):
    """Insert delete_dir actions, avoiding duplicates."""
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    planned = 0

    for d in directories:
        # Prevent duplicate pending delete_dir actions
        exists = c.execute("""
            SELECT 1 FROM actions
            WHERE action='delete_dir'
              AND src_path=?
              AND status='pending'
        """, (str(d),)).fetchone()

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
    """Execute pending delete_dir actions safely."""
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
                for item in p.iterdir():
                    if item.is_file() and is_small_image(item):
                        item.unlink()

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

    if args.apply and args.dry_run:
        raise SystemExit(MSG_MODE_CONFLICT)

    db_path = Path(args.db)
    target = Path(args.target)

    if not target.is_dir():
        raise SystemExit(MSG_INVALID_TARGET)

    candidates, skipped = scan_deletable_dirs(target)

    if args.dry_run or not args.apply:
        print({
            "key": MSG_SCAN_RESULT,
            "params": {"count": len(candidates)}
        })
        print({
            "key": MSG_SKIPPED_DIRS,
            "params": {"count": skipped}
        })
        for d in candidates:
            print({
                "key": MSG_CANDIDATE_DIR,
                "params": {"path": str(d)}
            })
        return

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
# ---------------- API SNIPPETS ----------------