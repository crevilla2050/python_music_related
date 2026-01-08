#!/usr/bin/env python3
"""
ingest_album_art.py

Album art discovery stage (Layer 1 – knowledge).

Discovers album art from:
- embedded tags
- common external image files

Stores descriptive metadata only.
"""

import os
import sqlite3
import hashlib
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from PIL import Image
from mutagen import File as MutagenFile

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    raise SystemExit("[ERROR] MUSIC_DB not set")


IMAGE_EXTS = {".jpg", ".jpeg", ".png"}
COMMON_NAMES = {"cover", "folder", "front", "album"}


def utcnow():
    return datetime.now(timezone.utc).isoformat()


def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def image_hash(data: bytes):
    return hashlib.sha256(data).hexdigest()


def ingest():
    conn = connect_db()
    c = conn.cursor()

    files = c.execute("""
        SELECT DISTINCT
            album_artist,
            album,
            is_compilation,
            original_path
        FROM files
        WHERE album IS NOT NULL
    """).fetchall()

    print(f"[INFO] Scanning album art for {len(files)} files")

    seen = set()

    for f in files:
        audio_path = Path(f["original_path"])
        album_dir = audio_path.parent

        # ---------- Embedded art ----------
        try:
            audio = MutagenFile(audio_path, easy=False)
            if audio and hasattr(audio, "tags"):
                for tag in audio.tags.values():
                    if hasattr(tag, "data"):
                        data = tag.data
                        h = image_hash(data)
                        if h in seen:
                            continue

                        img = Image.open(Path(audio_path))
                        width, height = img.size

                        c.execute("""
                            INSERT OR IGNORE INTO album_art (
                                album_artist, album, is_compilation,
                                image_hash, source, confidence,
                                mime, width, height,
                                created_at
                            )
                            VALUES (?, ?, ?, ?, 'embedded', 0.9, ?, ?, ?, ?)
                        """, (
                            f["album_artist"], f["album"], f["is_compilation"],
                            h, img.format, width, height, utcnow()
                        ))

                        seen.add(h)
        except Exception:
            pass

        # ---------- External art ----------
        for img_path in album_dir.iterdir():
            if img_path.suffix.lower() not in IMAGE_EXTS:
                continue

            if img_path.stem.lower() not in COMMON_NAMES:
                continue

            try:
                data = img_path.read_bytes()
                h = image_hash(data)
                if h in seen:
                    continue

                img = Image.open(img_path)
                width, height = img.size

                c.execute("""
                    INSERT OR IGNORE INTO album_art (
                        album_artist, album, is_compilation,
                        image_hash, source, confidence,
                        mime, width, height,
                        created_at
                    )
                    VALUES (?, ?, ?, ?, 'external', 0.8, ?, ?, ?, ?)
                """, (
                    f["album_artist"], f["album"], f["is_compilation"],
                    h, img.format, width, height, utcnow()
                ))

                seen.add(h)

            except Exception:
                continue

    conn.commit()
    conn.close()
    print("[✓] Album art ingestion complete")


if __name__ == "__main__":
    ingest()
