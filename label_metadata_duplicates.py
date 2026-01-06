#!/usr/bin/env python3
"""
label_metadata_duplicates.py

Detect near-duplicates using metadata similarity.
Uses confidence tiers and populates duplicates table.
"""

import sqlite3
import os
from rapidfuzz import fuzz
from datetime import datetime, timezone
from dotenv import load_dotenv

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB", "music_consolidation.db")

HIGH = 0.90
MEDIUM = 0.75


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def similarity(a, b):
    if not a or not b:
        return 0
    return fuzz.ratio(a, b) / 100.0


def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # NEW
    c.execute("PRAGMA foreign_keys = ON")

    c.execute("""
        SELECT
            id,
            artist,
            album_artist,
            album,
            title,
            duration,
            is_compilation
        FROM files
        WHERE status='pending'
    """)
    rows = c.fetchall()

    seen = set()
    labeled = 0

    print(f"[INFO] Found {len(rows)} candidates")

    for i, r1 in enumerate(rows):
        for r2 in rows[i+1:]:
            key = tuple(sorted((r1["id"], r2["id"])))
            if key in seen:
                continue

            # NEW: compilation safety
            if r1["is_compilation"] != r2["is_compilation"]:
                continue

            # Prefer album_artist when available
            a1 = r1["album_artist"] or r1["artist"]
            a2 = r2["album_artist"] or r2["artist"]

            artist_sim = similarity(a1, a2)
            title_sim = similarity(r1["title"], r2["title"])

            score = (artist_sim * 0.5) + (title_sim * 0.5)

            if r1["duration"] and r2["duration"]:
                if abs(r1["duration"] - r2["duration"]) <= 3:
                    score += 0.05

            if score < MEDIUM:
                continue

            confidence = (
                0.95 if score >= HIGH else
                0.80 if score >= MEDIUM else
                0.65
            )

            canonical = min(r1["id"], r2["id"])
            dup = max(r1["id"], r2["id"])

            c.execute("""
                INSERT OR IGNORE INTO duplicates
                (file1_id, file2_id, reason, confidence, created_at)
                VALUES (?, ?, 'metadata', ?, ?)
            """, (canonical, dup, confidence, utcnow()))

            c.execute("""
                UPDATE files
                SET status='suspected_duplicate', action='archive'
                WHERE id=?
            """, (dup,))

            seen.add(key)
            labeled += 1

    conn.commit()
    conn.close()

    print(f"[✓] Metadata duplicates labeled: {labeled}")


if __name__ == "__main__":
    main()
