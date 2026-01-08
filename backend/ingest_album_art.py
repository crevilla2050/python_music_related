#!/usr/bin/env python3
"""
backend/ingest_album_art.py

Album art discovery stage (Layer 1 – Knowledge).

This module scans the media library for potential album artwork and
records descriptive metadata in the application's database. It does not
embed images into audio files or write normalized images to disk — it
only discovers candidates and stores their hashes, dimensions, mime and
a confidence score so downstream UI or operators can decide which art
to apply.

Discovery includes two conservative sources:
- Embedded artwork found inside audio file tags
- Commonly-named external images located beside audio files

All user-facing messages are returned as i18n keys; actual text is
loaded from the UI JSON files.
"""

import os
import sqlite3
import hashlib
import json
from pathlib import Path
from datetime import datetime, timezone
from dotenv import load_dotenv
from PIL import Image
from mutagen import File as MutagenFile


# ================= ENV / I18N =================

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    raise SystemExit("MUSIC_DB_NOT_SET")

LANG = os.getenv("PEDRO_LANG", "en")
I18N_PATH = Path("music-ui/src/i18n") / f"{LANG}.json"

def load_messages():
    if I18N_PATH.exists():
        try:
            with open(I18N_PATH, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

_MESSAGES = load_messages()

def msg(key: str) -> str:
    return _MESSAGES.get(key, key)


# ================= CONFIG =================

IMAGE_EXTS = {".jpg", ".jpeg", ".png"}
COMMON_NAMES = {"cover", "folder", "front", "album"}


# ================= HELPERS =================

def utcnow():
    """Return current UTC time as an ISO-8601 string for DB timestamps."""
    return datetime.now(timezone.utc).isoformat()

def connect_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def image_hash(data: bytes):
    """Return a SHA-256 hex digest for `data`.

    We store hashes rather than raw bytes so the DB remains small and
    comparisons between embedded and external images are efficient.
    """
    return hashlib.sha256(data).hexdigest()


# ================= CORE =================

def ingest():
    conn = connect_db()
    c = conn.cursor()

    # Select all files that belong to an album so we can search for art
    files = c.execute("""
        SELECT DISTINCT
            album_artist,
            album,
            is_compilation,
            original_path
        FROM files
        WHERE album IS NOT NULL
    """).fetchall()

    print(msg("INGEST_ART_SCAN_START").format(count=len(files)))

    # `seen` keeps a set of image hashes we've already recorded to avoid
    # duplicate entries when the same image appears embedded and as an
    # external file.
    seen = set()

    for f in files:
        audio_path = Path(f["original_path"])
        album_dir = audio_path.parent

        # ---------- Embedded art ----------
        try:
            # Mutagen returns container-specific tag structures; some tag
            # objects expose a `data` attribute containing raw image bytes.
            audio = MutagenFile(audio_path, easy=False)
            if audio and hasattr(audio, "tags"):
                for tag in audio.tags.values():
                    if hasattr(tag, "data"):
                        data = tag.data
                        h = image_hash(data)
                        if h in seen:
                            continue

                        # Determine image format and dimensions for the DB
                        img = Image.open(audio_path)
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
            # Be permissive: skip files that Mutagen or PIL cannot parse
            pass

        # ---------- External art ----------
        for img_path in album_dir.iterdir():
            if img_path.suffix.lower() not in IMAGE_EXTS:
                continue

            # Only consider commonly named artwork files (cover, folder, etc.)
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
                # Skip unreadable or corrupt image files
                continue

    conn.commit()
    conn.close()

    print(msg("INGEST_ART_COMPLETE"))


if __name__ == "__main__":
    ingest()
# ---------------- END OF FILE ----------------