#!/usr/bin/env python3
"""
pedro_enrich_album_art.py

Runs Pedro album-art enrichment per album cluster
and stores suggestions into album_art table.
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
    raise SystemExit("[ERROR] MUSIC_DB not set")

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def hash_image(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # --- fetch album clusters ---
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
        paths = row["paths"].split(",")

        result = pedro_enrich_cluster(
            album_artist=row["album_artist"],
            album=row["album"],
            is_compilation=row["is_compilation"],
            source_paths=paths,
        )

        if not result["success"]:
            continue

        art = result["art"]
        img = art.get("image_bytes")
        if not img:
            continue

        img_hash = hash_image(img)

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
