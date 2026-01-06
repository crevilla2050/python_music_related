#!/usr/bin/env python3
"""
sanity_check.py

Sanity checks for music_consolidation database (NEW SCHEMA)

Checks:
- files table exists
- total row count
- action distribution
- status distribution
- fingerprint duplicate clusters
- sha256 duplicate clusters
- inconsistent status/action combinations
"""

import sqlite3
from collections import Counter
from pathlib import Path
import os
from dotenv import load_dotenv

# ===================== ENV =====================

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    raise SystemExit("[ERROR] MUSIC_DB not set in .env")

# ===================== Helpers =====================

def print_header(title):
    print("\n" + "=" * 60)
    print(title)
    print("=" * 60)


def connect_db():
    if not Path(DB_PATH).exists():
        raise SystemExit(f"[ERROR] DB not found: {DB_PATH}")
    return sqlite3.connect(DB_PATH)


def check_total_rows(conn):
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM files")
    total = c.fetchone()[0]
    print(f"[DB] Total rows in files table: {total}")
    return total


def check_action_distribution(conn):
    c = conn.cursor()
    c.execute("SELECT action FROM files")
    actions = [r[0] for r in c.fetchall()]
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
    print("\n[DB] Fingerprint duplicate clusters:")
    if not rows:
        print("  None found")
        return
    for fp, count in rows:
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
    print("\n[DB] SHA-256 duplicate clusters:")
    if not rows:
        print("  None found")
        return
    for sha, count in rows:
        print(f"  sha256={sha[:12]}…  count={count}")


def check_inconsistent_rows(conn):
    """
    Look for rows that don't make logical sense.
    """
    c = conn.cursor()

    print("\n[DB] Inconsistent rows:")

    problems = 0

    # duplicate but marked move
    c.execute("""
        SELECT COUNT(*) FROM files
        WHERE status='duplicate' AND action='move'
    """)
    n = c.fetchone()[0]
    if n:
        print(f"  [!] {n} rows: status=duplicate but action=move")
        problems += n

    # unique but archived/deleted
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
