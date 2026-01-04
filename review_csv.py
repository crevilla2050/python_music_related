#!/usr/bin/env python3
"""
review_csv.py

CSV-based bulk reviewer for music_consolidation.db

Features:
- Export DB rows to CSV with root + tree-path separation
- Import edited CSV back into SQLite
- Supports disk migration via root editing
- Creates missing directories (logged in notes)
- Safe for large libraries (30k+ rows)
"""

import sqlite3
import csv
import argparse
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = "music_consolidation.db"
CSV_PATH = "review.csv"

VALID_ACTIONS = {"move", "skip", "delete", "archive"}
DEFAULT_ACTION = "move"


# -------------------- helpers --------------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()


def connect_db():
    return sqlite3.connect(DB_PATH)


def split_path(full_path: str | None, root_dir: str | None):
    if not full_path:
        return "", ""

    full = Path(full_path).resolve()

    if root_dir:
        root = Path(root_dir).expanduser().resolve()
        try:
            rel = full.relative_to(root)
            # current_root should be the provided fixed root, tree path the variable part
            return str(root), str(rel)
        except ValueError:
            # Not under the provided root: still return the fixed root and the full path as variable part
            return str(root), str(full)

    # fallback if no root_dir: return parent as root and filename as tree path
    return str(full.parent), full.name



def join_path(root: str, tree: str):
    if not root:
        return tree
    return str(Path(root) / tree)


# -------------------- export --------------------

def export_csv(root_dir: str | None, only_pending=False):
    # Ensure DB file exists to avoid confusing sqlite auto-create behavior
    db_file = Path(DB_PATH)
    if not db_file.exists():
        print(f"[!] Database not found: {DB_PATH}\n    Run the analysis to create the DB (e.g. consolidate_music.py --src <SRC> --lib <LIB>)")
        return

    conn = connect_db()
    c = conn.cursor()

    query = """
        SELECT id, original_path, recommended_path, action, status, notes
        FROM files
    """

    try:
        if only_pending:
            query += " WHERE action = ?"
            c.execute(query, (DEFAULT_ACTION,))
        else:
            c.execute(query)
    except sqlite3.OperationalError as e:
        # Provide a helpful error when the expected schema/table doesn't exist
        if "no such table" in str(e).lower():
            print(f"[!] The database exists but does not contain the expected table 'files'.\n    You may need to run the analysis step to populate the DB (consolidate_music.py).")
            conn.close()
            return
        raise

    rows = c.fetchall()
    conn.close()

    with open(CSV_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "id",
            "action",
            "status",
            "current_root",
            "current_tree_path",
            "proposed_root",
            "proposed_tree_path",
            "notes"
        ])

        for fid, orig, rec, action, status, notes in rows:
            cr, ct = split_path(orig, root_dir)
            pr, pt = split_path(rec if rec else orig, root_dir)

            # Ensure filename is only at end of tree path
            ct = str(Path(ct))
            pt = str(Path(pt))

            writer.writerow([
                fid,
                action or DEFAULT_ACTION,
                status,
                cr,
                ct,
                pr,
                pt,
                notes or ""
            ])

    print(f"[✓] CSV exported to {CSV_PATH}")


# -------------------- import --------------------

def import_csv(create_dirs=True):
    conn = connect_db()
    c = conn.cursor()

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)

        for row in reader:
            fid = int(row["id"])
            action = row["action"].strip().lower()
            notes = row.get("notes", "").strip()

            if action not in VALID_ACTIONS:
                print(f"[!] Skipping ID {fid}: invalid action '{action}'")
                continue

            proposed_path = join_path(
                row["proposed_root"].strip(),
                row["proposed_tree_path"].strip()
            )

            note_log = []

            if create_dirs and proposed_path:
                target_dir = Path(proposed_path).parent
                if not target_dir.exists():
                    target_dir.mkdir(parents=True, exist_ok=True)
                    note_log.append(f"created_dir:{target_dir}")

            if notes:
                note_log.append(notes)

            final_notes = " | ".join(note_log)

            c.execute("""
                UPDATE files
                SET action = ?, recommended_path = ?, notes = ?, last_update = ?
                WHERE id = ?
            """, (
                action,
                proposed_path,
                final_notes,
                utcnow(),
                fid
            ))

    conn.commit()
    conn.close()
    print("[✓] CSV changes applied to database")


# -------------------- CLI --------------------

def main():
    parser = argparse.ArgumentParser(description="CSV reviewer for music consolidation DB")
    parser.add_argument("root", nargs="?", help="Base root directory for path splitting (positional)")
    parser.add_argument("--export", action="store_true", help="Export DB to CSV")
    parser.add_argument("--import", dest="do_import", action="store_true", help="Import CSV into DB")
    parser.add_argument("--root-dir", help="Base root directory for path splitting")
    parser.add_argument("--only-pending", action="store_true", help="Export only default-action rows")

    args = parser.parse_args()

    # Prefer positional `root` if provided, otherwise fall back to `--root-dir`
    chosen_root = args.root if args.root else args.root_dir

    if args.export:
        export_csv(chosen_root, args.only_pending)
    elif args.do_import:
        import_csv()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
