#!/usr/bin/env python3
"""
genre_normalizer_cli.py

Interactive CLI for normalizing genre tags.

Responsibilities:
- Discover unmapped genre tokens from files.genre
- Ask the user how to handle each token
- Persist mappings in genre_mappings and genres tables
- NEVER mutates files directly (knowledge layer only)

Design goals:
- Human-in-the-loop
- Deterministic
- Resumable
- Typo-resistant
"""

import sqlite3
import unicodedata
import sys
from datetime import datetime, timezone

# ---------------- utilities ----------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()


def normalize_token(s: str) -> str:
    """
    Normalize a genre token for comparison:
    - lowercase
    - strip
    - remove accents
    """
    s = s.strip().lower()
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def suggest_canonical(raw: str) -> str:
    """
    Suggest a canonical genre name from raw token.
    Title-cased, accents preserved if present.
    """
    return raw.strip().title()


# ---------------- core logic ----------------

def fetch_unmapped_tokens(c):
    """
    Return a list of distinct genre tokens from files.genre
    that are not yet present in genre_mappings.
    """
    rows = c.execute("""
        SELECT DISTINCT genre
        FROM files
        WHERE genre IS NOT NULL
          AND TRIM(genre) != ''
          AND LOWER(TRIM(genre)) NOT IN (
              SELECT raw_token FROM genre_mappings
          )
        ORDER BY genre
    """).fetchall()

    return [r[0] for r in rows]


def ensure_genre(c, canonical_name: str) -> int:
    """
    Ensure a genre exists in genres table.
    Return genre_id.
    """
    normalized = normalize_token(canonical_name)

    row = c.execute("""
        SELECT id FROM genres
        WHERE normalized_name=?
    """, (normalized,)).fetchone()

    if row:
        return row[0]

    c.execute("""
        INSERT INTO genres (name, normalized_name, source, created_at)
        VALUES (?, ?, 'user', ?)
    """, (canonical_name, normalized, utcnow()))

    return c.lastrowid


def insert_mapping(c, raw_token, genre_id=None):
    """
    Insert a mapping for a raw genre token.
    genre_id = NULL means ignored.
    """
    c.execute("""
        INSERT OR IGNORE INTO genre_mappings (
            raw_token,
            normalized_token,
            genre_id,
            source,
            created_at
        )
        VALUES (?, ?, ?, 'user', ?)
    """, (
        raw_token,
        normalize_token(raw_token),
        genre_id,
        utcnow()
    ))


# ---------------- interactive CLI ----------------

def main(db_path, dry_run=False):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print("\n=== Genre Normalization ===")
    print(f"Database: {db_path}")
    print(f"Mode: {'DRY-RUN' if dry_run else 'APPLY'}\n")

    tokens = fetch_unmapped_tokens(c)

    if not tokens:
        print("No unmapped genre tokens found.")
        return

    applied = 0

    try:
        for raw in tokens:
            normalized = normalize_token(raw)
            suggestion = suggest_canonical(raw)

            print(f"\nUnmapped genre token:")
            print(f"  Raw:        '{raw}'")
            print(f"  Normalized: '{normalized}'")

            choice = input("    [m]ap  [i]gnore  [s]kip ? ").strip().lower()

            if choice == "s":
                print("    skipped")
                continue

            if choice == "i":
                if not dry_run:
                    insert_mapping(c, raw, None)
                applied += 1
                print("    ✓ ignored")
                continue

            if choice == "m":
                prompt = f"    Canonical genre name [{suggestion}]: "
                canonical = input(prompt).strip()
                if not canonical:
                    canonical = suggestion

                if not dry_run:
                    genre_id = ensure_genre(c, canonical)
                    insert_mapping(c, raw, genre_id)

                applied += 1
                print(f"    ✓ mapped → {canonical}")
                continue

            print("    invalid choice, skipping")

    except KeyboardInterrupt:
        print("\n\nInterrupted by user. Progress preserved.")

    finally:
        if not dry_run:
            conn.commit()
        conn.close()

    print(f"\nApplied relations: {applied}")
