#!/usr/bin/env python3
"""
sanity_check.py

Small utility to perform sanity checks on the SQLite staging database
created by the consolidation pipeline. This script is intentionally
lightweight and prints human-friendly summaries that help an operator
quickly spot obvious problems before moves/renames are applied.

Checks performed:
- files table row count
- distribution of `action` and `status` values
- clusters of identical fingerprints (perceptual duplicates)
- clusters of identical SHA-256 hashes (exact duplicates)
- simple logical inconsistencies between `status` and `action`

Usage:
1. Ensure the consolidation script has written `MUSIC_DB` into `.env`.
2. Run this script; it will read the database and print summaries.

This file focuses on clarity rather than performance: queries fetch
small result sets and print concise diagnostics for manual inspection.
"""

import sqlite3
from collections import Counter
from pathlib import Path
import os
from dotenv import load_dotenv

# ===================== ENV =====================

load_dotenv()

# The consolidation pipeline writes the active DB filename to `.env`
# as `MUSIC_DB`. This allows helper scripts (like this one) to find
# and open the current database without requiring complex CLI flags.
DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    # Fail early with a clear message rather than using a default.
    raise SystemExit("[ERROR] MUSIC_DB not set in .env")

# ===================== Helpers =====================

def print_header(title):
    # Nicely formatted title for each run so outputs are easy to scan.
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def connect_db():
    # Verify the DB file exists before attempting to connect. A common
    # mistake is running this script from a different working directory
    # where `.env` was created, so an explicit check produces a clearer
    # error message than a raw sqlite3 exception.
    if not Path(DB_PATH).exists():
        raise SystemExit(f"[ERROR] DB not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def check_total_rows(conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM files")
    total = c.fetchone()[0]
    # Report the total number of rows stored in the `files` table.
    # Zero rows most often means the analysis is still running or the
    # wrong DB was selected.
    print(f"[DB] Total rows in files table: {total}")
    return total


def check_action_distribution(conn):
    c = conn.cursor()
    c.execute("SELECT action FROM files")
    actions = [r[0] for r in c.fetchall()]
    # Show how many rows are marked with each `action` (e.g. move,
    # archive, delete). This helps verify that the consolidation
    # policy is producing the expected distribution before any file
    # operations run.
    print("\n[DB] Action distribution:")
    if not actions:
        print("  (no rows)")
        return
    for k, v in Counter(actions).items():
        print(f"  {k:<10} {v}")


def check_status_distribution(conn):
    c = conn.cursor()
    c.execute("SELECT status FROM files")
    statuses = [r[0] for r in c.fetchall()]
    # Show the `status` histogram (e.g. unique, duplicate). This
    # complements the action distribution and can reveal unexpected
    # outcomes from the duplicate detection stage.
    print("\n[DB] Status distribution:")
    if not statuses:
        print("  (no rows)")
        return
    for k, v in Counter(statuses).items():
        print(f"  {k:<12} {v}")


def check_fingerprint_duplicates(conn):
    c = conn.cursor()
    c.execute("""
        SELECT fingerprint, COUNT(*)
        FROM files
        WHERE fingerprint IS NOT NULL
        GROUP BY fingerprint
        HAVING COUNT(*) > 1
    """)
    rows = c.fetchall()
    # List groups of files that share the same perceptual fingerprint.
    # These clusters suggest perceptual duplicates — different file
    # encodings, bitrates, or small edits of the same audio content.
    print("\n[DB] Fingerprint duplicate clusters:")
    if not rows:
        print("  None found")
        return
    for fp, count in rows:
        # Only show a short prefix of the fingerprint hash to keep the
        # output readable; the full value is stored in the DB if needed.
        print(f"  fingerprint={fp[:12]}…  count={count}")


def check_sha256_duplicates(conn):
    c = conn.cursor()
    c.execute("""
        SELECT sha256, COUNT(*)
        FROM files
        WHERE sha256 IS NOT NULL
        GROUP BY sha256
        HAVING COUNT(*) > 1
    """)
    rows = c.fetchall()
    # Exact-file duplicates (identical bytes) are grouped by SHA-256.
    # These are safe to dedupe automatically, but it's good to spot
    # any unexpectedly large clusters which may indicate mass repeats.
    print("\n[DB] SHA-256 duplicate clusters:")
    if not rows:
        print("  None found")
        return
    for sha, count in rows:
        print(f"  sha256={sha[:12]}…  count={count}")


def check_inconsistent_rows(conn):
    """
    Run simple logical consistency checks across `status` and `action`.

    These checks are intentionally conservative: they flag rows that
    almost certainly indicate user error or a logic bug in earlier
    pipeline stages (for example, a duplicate flagged as 'move').
    """
    c = conn.cursor()

    print("\n[DB] Inconsistent rows:")

    problems = 0

    # Case 1: a row is marked as a duplicate but the selected action
    # is `move`. Typically, duplicates should be archived or deleted
    # rather than moved into the canonical library.
    c.execute("""
        SELECT COUNT(*) FROM files
        WHERE status='duplicate' AND action='move'
    """)
    n = c.fetchone()[0]
    if n:
        print(f"  [!] {n} rows: status=duplicate but action=move")
        problems += n

    # Case 2: a row is marked as unique but the selected action is to
    # archive or delete it. That combination is suspicious and worth
    # human review before any destructive action.
    c.execute("""
        SELECT COUNT(*) FROM files
        WHERE status='unique' AND action IN ('archive','delete')
    """)
    n = c.fetchone()[0]
    if n:
        print(f"  [!] {n} rows: status=unique but action=archive/delete")
        problems += n

    if problems == 0:
        print("  No inconsistencies found")


def main():
    # Toplevel runner: open the DB, perform each check in sequence, and
    # close the connection. The script prints warnings rather than
    # raising exceptions so it can be used interactively by operators.
    print_header("MUSIC CONSOLIDATION — SANITY CHECK (ACTIVE DB)")
    print(f"[DB] Using: {DB_PATH}")

    conn = connect_db()

    total = check_total_rows(conn)
    if total == 0:
        print("\n[WARN] Database is empty — analysis may still be running")

    check_action_distribution(conn)
    check_status_distribution(conn)
    check_fingerprint_duplicates(conn)
    check_sha256_duplicates(conn)
    check_inconsistent_rows(conn)

    conn.close()
    print("\n[✓] Sanity check complete")


if __name__ == "__main__":
    main()
