#!/usr/bin/env python3
"""
find_duplicates.py

Pedro Organiza – Duplicate Detection (Planning Only)

Detects:
- Exact duplicates (content hash)
- Fuzzy duplicates (artist + title similarity)

NEVER moves files.
NEVER deletes files.
ONLY writes planning info to the database.

Layer: Knowledge → Planning
"""

import os
import sys
import hashlib
import sqlite3
from difflib import SequenceMatcher
from pathlib import Path
from mutagen import File as MutagenFile

from backend.i18n.messages import msg

SUPPORTED_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg"}


# ---------------- HASHING ----------------

def compute_hash(path: Path, block_size=65536):
    h = hashlib.sha1()
    try:
        with open(path, "rb") as f:
            while chunk := f.read(block_size):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


# ---------------- TAGS ----------------

def get_audio_tags(path: Path):
    try:
        audio = MutagenFile(path, easy=True)
        if not audio:
            return "", ""
        artist = audio.get("artist", [""])[0]
        title = audio.get("title", [""])[0]
        return artist or "", title or ""
    except Exception:
        return "", ""


def is_fuzzy_match(a, b, threshold=0.85):
    a_artist, a_title = a
    b_artist, b_title = b
    if not all([a_artist, a_title, b_artist, b_title]):
        return False

    artist_ratio = SequenceMatcher(None, a_artist.lower(), b_artist.lower()).ratio()
    title_ratio = SequenceMatcher(None, a_title.lower(), b_title.lower()).ratio()

    return artist_ratio >= threshold and title_ratio >= threshold


# ---------------- MAIN LOGIC ----------------

def find_duplicates(db_path, source_dir):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    print(msg("DUP_SCAN_START", path=source_dir))

    seen = {}

    for root, _, files in os.walk(source_dir):
        for name in files:
            path = Path(root) / name
            if path.suffix.lower() not in SUPPORTED_EXTS:
                continue

            print(msg("SCANNING_FILE", path=str(path)))

            file_hash = compute_hash(path)
            if not file_hash:
                continue

            tags = get_audio_tags(path)
            matched = False

            for seen_path, data in seen.items():
                if file_hash == data["hash"]:
                    reason = "hash"
                    confidence = 1.0
                    matched = True
                elif is_fuzzy_match(tags, data["tags"]):
                    reason = "fuzzy"
                    confidence = 0.85
                    matched = True
                else:
                    continue

                # Resolve file IDs
                f1 = c.execute(
                    "SELECT id FROM files WHERE original_path=?",
                    (str(seen_path),)
                ).fetchone()

                f2 = c.execute(
                    "SELECT id FROM files WHERE original_path=?",
                    (str(path),)
                ).fetchone()

                if f1 and f2:
                    c.execute("""
                        INSERT OR IGNORE INTO duplicates
                        (file1_id, file2_id, reason, confidence, created_at)
                        VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
                    """, (f1["id"], f2["id"], reason, confidence))

                print(msg("DUP_FOUND", path=str(path), reason=reason))
                matched = True
                break

            if not matched:
                seen[path] = {
                    "hash": file_hash,
                    "tags": tags
                }

    conn.commit()
    conn.close()
    print(msg("DUP_SCAN_COMPLETE"))


# ---------------- CLI ----------------

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(msg("FIND_DUP_USAGE"))
        sys.exit(1)

    db_path = sys.argv[1]
    source_dir = sys.argv[2]

    if not os.path.isdir(source_dir):
        print(msg("INVALID_DIRECTORY", path=source_dir))
        sys.exit(1)

    find_duplicates(db_path, source_dir)
