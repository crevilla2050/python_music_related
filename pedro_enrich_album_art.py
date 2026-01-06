#!/usr/bin/env python3
"""
pedro_enrich_album_art.py

Driver script that runs the Pedro album-art enrichment step for each
album-level cluster discovered in the staging `files` table.

Why this module exists:
- The consolidation pipeline groups audio files into album clusters
    (by album artist + album + compilation flag). This module takes
    those clusters, asks the enrichment engine for an album-art
    suggestion, and persists high-confidence candidates into the
    `album_art` table as non-destructive suggestions.

Notes on behavior:
- This script is advisory — it never mutates audio files or embeds
    images. Results are marked `suggested` so they can be reviewed and
    applied later by a human or an automated policy.
"""

import sqlite3
import os
import hashlib
from datetime import datetime, timezone
from dotenv import load_dotenv

from new_pedro_tagger import pedro_enrich_cluster

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    # Fail fast with a clear message: the rest of the script expects
    # an environment variable `MUSIC_DB` pointed at the SQLite DB file
    # created by the consolidation pipeline. Writing to `.env` is the
    # mechanism used elsewhere in this project to communicate the
    # active DB filename to helper scripts.
    raise SystemExit("[ERROR] MUSIC_DB not set")

def utcnow():
    """
    Return a reproducible UTC ISO timestamp string.

    Why: Suggested album-art rows are timestamped so consumers can
    show when suggestions were produced. Using UTC and ISO format
    keeps ordering and parsing straightforward across tools.
    """
    return datetime.now(timezone.utc).isoformat()

def hash_image(data: bytes) -> str:
    """
    Produce a SHA-256 hex digest for an image's bytes.

    Why: We store a compact `image_hash` in the DB to identify
    duplicate images and to avoid re-inserting identical suggestions
    from multiple runs or sources. Hashing keeps the DB storage
    small while enabling equality checks.
    """
    return hashlib.sha256(data).hexdigest()

def main():
    """
    Main entrypoint for the enrichment run.

    High-level flow:
    1. Connect to the staging DB and fetch album clusters. A cluster is
       identified by the triplet `(album_artist, album, is_compilation)`.
       We only consider rows where `album` is present because album-art
       is an album-level concept.
    2. For each cluster, call `pedro_enrich_cluster` to obtain an art
       suggestion (if any).
    3. If a suggestion includes image bytes, compute a hash and insert
       a `suggested` row into the `album_art` table using
       `INSERT OR IGNORE` to avoid duplicate entries.

    The script deliberately keeps operations simple and idempotent so
    it can be re-run multiple times without producing duplicate rows.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # --- fetch album clusters ---
    # We group by album metadata and concatenate file paths so the
    # enrichment engine can inspect example files from each cluster.
    c.execute("""
        SELECT
            album_artist,
            album,
            is_compilation,
            GROUP_CONCAT(original_path) AS paths
        FROM files
        WHERE album IS NOT NULL
        GROUP BY album_artist, album, is_compilation
    """)

    clusters = c.fetchall()
    print(f"[INFO] Found {len(clusters)} album clusters")

    created = 0

    for row in clusters:
        # `paths` is a comma-separated list of example file paths from
        # the cluster; pass them to the enrichment engine so it can
        # look for sibling images or derive context.
        paths = row["paths"].split(",")

        result = pedro_enrich_cluster(
            album_artist=row["album_artist"],
            album=row["album"],
            is_compilation=row["is_compilation"],
            source_paths=paths,
        )

        # Only persist successful suggestions that include raw image
        # bytes. Many enrichment runs will return a `missing` result
        # (e.g., network lookup disabled) and those are safely ignored.
        if not result["success"]:
            continue

        art = result["art"]
        img = art.get("image_bytes")
        if not img:
            continue

        img_hash = hash_image(img)

        # Use INSERT OR IGNORE so identical suggestions (by unique
        # constraint on image_hash/album fields) are not duplicated on
        # repeated runs. The `status` is set to 'suggested' for later
        # human review or automated selection.
        c.execute("""
            INSERT OR IGNORE INTO album_art (
                album_artist,
                album,
                is_compilation,
                image_hash,
                source,
                confidence,
                mime,
                status,
                created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, 'suggested', ?)
        """, (
            row["album_artist"],
            row["album"],
            row["is_compilation"],
            img_hash,
            art["source"],
            art["confidence"],
            art["mime"],
            utcnow(),
        ))

        created += 1

    conn.commit()
    conn.close()

    print(f"[✓] Pedro album-art suggestions created: {created}")

if __name__ == "__main__":
    main()
