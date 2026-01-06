#!/usr/bin/env python3
"""
consolidate_music.py

Music consolidation pipeline with:
- SHA-256 identity
- OPTIONAL Chromaprint fingerprinting
- Canonical resolution
- Metadata ingestion
- SQLite staging DB (timestamped)
"""

import os
import sys
import sqlite3
import json
import hashlib
import subprocess
import re
import unicodedata
import logging
from datetime import datetime, timezone
from pathlib import Path

from mutagen import File as MutagenFile
from rapidfuzz import fuzz
from dotenv import load_dotenv

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import chromaprint
except Exception:
    chromaprint = None

# ================= CONFIG =================
# Configuration for supported file types and optional fingerprinting.
# - `SUPPORTED_EXTS` lists audio file suffixes the script will process.
# - `ENABLE_CHROMAPRINT` and `FP_SECONDS` control fingerprint generation.

SUPPORTED_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".aac", ".opus"}

ENABLE_CHROMAPRINT = True
FP_SECONDS = 90

load_dotenv()

# ========================================

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

def log(msg):
    logging.info(msg)

def maybe_progress(it, desc=None, enable=False):
    # Wrap an iterator with tqdm progress bar when requested and available.
    # This keeps the function safe to call even if `tqdm` isn't installed.
    if enable and tqdm:
        return tqdm(it, desc=desc)
    return it

# ================= DATABASE =================

def create_db():
    # Create a timestamped SQLite DB to stage analysis results.
    # The DB filename is written into a local `.env` file so other
    # helper scripts can discover it via environment variables.
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    db_name = f"music_consolidation_{ts}.db"

    with open(".env", "w") as f:
        f.write(f"MUSIC_DB={db_name}\n")

    log(f"Database created: {db_name}")
    log(f".env updated: MUSIC_DB={db_name}")

    conn = sqlite3.connect(db_name)
    c = conn.cursor()

    c.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_path TEXT UNIQUE,

        sha256 TEXT,
        size_bytes INTEGER,

        artist TEXT,
        album_artist TEXT,
        album TEXT,
        title TEXT,
        track TEXT,

        duration REAL,
        bitrate INTEGER,

        fingerprint TEXT,

        is_compilation INTEGER DEFAULT 0,

        status TEXT DEFAULT 'pending',
        action TEXT NOT NULL DEFAULT 'move',

        recommended_path TEXT,

        first_seen TEXT,
        last_update TEXT,
        notes TEXT
    );

    CREATE TABLE IF NOT EXISTS duplicates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file1_id INTEGER NOT NULL,
        file2_id INTEGER NOT NULL,
        reason TEXT NOT NULL,
        confidence REAL,
        created_at TEXT NOT NULL,
        FOREIGN KEY(file1_id) REFERENCES files(id),
        FOREIGN KEY(file2_id) REFERENCES files(id)
    );
    
    CREATE TABLE IF NOT EXISTS album_art (
        id INTEGER PRIMARY KEY AUTOINCREMENT,

        album_artist TEXT,
        album TEXT,
        is_compilation INTEGER DEFAULT 0,

        image_hash TEXT,
        source TEXT,
        confidence REAL,

        mime TEXT,
        width INTEGER,
        height INTEGER,

        status TEXT DEFAULT 'suggested',

        created_at TEXT NOT NULL
    );

    CREATE INDEX IF NOT EXISTS idx_album_art_album ON album_art(album_artist, album, is_compilation);
    CREATE INDEX IF NOT EXISTS idx_album_art_status ON album_art(status);
    CREATE INDEX IF NOT EXISTS idx_files_sha ON files(sha256);
    CREATE INDEX IF NOT EXISTS idx_files_fp ON files(fingerprint);
    CREATE INDEX IF NOT EXISTS idx_files_album_artist ON files(album_artist);
    CREATE INDEX IF NOT EXISTS idx_files_compilation ON files(is_compilation);
                    
    
    """)

    conn.commit()
    return conn, db_name

# ================= UTILITIES =================

def is_audio_file(p: Path):
    # True only for regular files with a supported audio extension.
    return p.is_file() and p.suffix.lower() in SUPPORTED_EXTS

def sha256_file(path: Path):
    # Compute a streaming SHA-256 hash of the file contents. Files are
    # read in 64KiB chunks to avoid loading large files entirely into RAM.
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def normalize_str(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    # Remove combining characters (accents) and trim surrounding whitespace.
    return "".join(c for c in s if not unicodedata.combining(c)).strip()

def sanitize_for_fs(s):
    # Make a string safe for filesystem use by removing or replacing
    # characters that are problematic in filenames on most platforms.
    if not s:
        return "Unknown"
    s = normalize_str(s)
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", s)
    return s.strip(" .")[:120]

def normalize_track(track_raw):
    if not track_raw:
        return None
    if isinstance(track_raw, list):
        track_raw = track_raw[0]
    track_raw = str(track_raw).strip()
    if "/" in track_raw:
        track_raw = track_raw.split("/", 1)[0]
    # Normalize numeric track numbers to two-digit strings (e.g. 3 -> '03').
    return f"{int(track_raw):02d}" if track_raw.isdigit() else None

def recommended_path_for(root, meta, ext):
    # Build a recommended canonical path for this track inside the
    # consolidation library. This is only a suggestion stored in the DB
    # and not enforced by this script — it helps later move/rename steps.
    artist = sanitize_for_fs(meta.get("album_artist") or meta.get("artist") or "Unknown Artist")
    album = sanitize_for_fs(meta.get("album") or "Unknown Album")
    title = sanitize_for_fs(meta.get("title") or meta.get("orig_name"))
    track = meta.get("track")
    fname = f"{track} - {title}{ext}" if track else f"{title}{ext}"
    return str(Path(root) / artist / album / fname)

# ================= METADATA =================

def extract_tags(path: Path):
    try:
        audio = MutagenFile(path, easy=True)
        raw = MutagenFile(path, easy=False)

        # Read common metadata fields using mutagen. `easy=True` gives
        # normalized, easy-to-use tag names while `easy=False` lets us
        # inspect raw tags for compilation flags.
        album_artist = audio.get("albumartist", [None])[0] if audio else None
        is_comp = 0

        if raw and hasattr(raw, "tags"):
            for k in raw.tags.keys():
                if str(k).lower() in ("tcmp", "compilation", "cpil"):
                    is_comp = 1
                    break

        return {
            "artist": audio.get("artist", [None])[0],
            "album_artist": album_artist,
            "album": audio.get("album", [None])[0],
            "title": audio.get("title", [None])[0],
            "track": normalize_track(audio.get("tracknumber", [None])[0]),
            "duration": getattr(audio.info, "length", None),
            "bitrate": getattr(audio.info, "bitrate", None),
            "is_compilation": is_comp,
            "orig_name": path.stem
        }
    except Exception:
        return {
            "artist": None,
            "album_artist": None,
            "album": None,
            "title": None,
            "track": None,
            "duration": None,
            "bitrate": None,
            "is_compilation": 0,
            "orig_name": path.stem
        }

# ================= INGEST =================

def analyze_files(src, lib, progress=False, with_fingerprint=False):
    conn, db_name = create_db()
    c = conn.cursor()

    audio_list = [p for p in Path(src).rglob("*") if is_audio_file(p)]
    log(f"Found {len(audio_list)} audio files")

    for p in maybe_progress(audio_list, "Analyzing", progress):
        meta = extract_tags(p)
        sha = sha256_file(p)
        # Optionally compute and store a compact fingerprint to help
        # detect perceptual duplicates (requires `ffmpeg` and the
        # `chromaprint` Python bindings). If fingerprinting is disabled
        # or unavailable, `fp` will be None.
        fp = compute_fingerprint(p) if with_fingerprint else None
        rec = recommended_path_for(lib, meta, p.suffix)
        now = datetime.now(timezone.utc).isoformat()

        c.execute("""
        INSERT INTO files (
            original_path, sha256, size_bytes, artist, album_artist,
            album, title, track, duration, bitrate, fingerprint,
            is_compilation, recommended_path, first_seen, last_update
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            str(p), sha, p.stat().st_size,
            meta["artist"], meta["album_artist"],
            meta["album"], meta["title"], meta["track"],
            meta["duration"], meta["bitrate"], fp,
            meta["is_compilation"], rec, now, now
        ))

    conn.commit()
    conn.close()
    log("Analysis complete")

# ================= FINGERPRINT =========
def compute_fingerprint(path: Path):
    # Compute an audio fingerprint using Chromaprint. This requires:
    # - `ffmpeg` available on PATH to decode audio to raw PCM
    # - the `chromaprint` Python bindings installed in the environment
    if not ENABLE_CHROMAPRINT or chromaprint is None:
        return None

    try:
        cmd = [
            "ffmpeg",
            "-v", "quiet",
            "-t", str(FP_SECONDS),
            "-i", str(path),
            "-f", "s16le",
            "-ac", "1",
            "-ar", "44100",
            "-"
        ]

        proc = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            check=True
        )

        pcm = proc.stdout
        if not pcm:
            return None

        fp = chromaprint.Fingerprinter(44100, 1)
        fp.feed(pcm)
        fingerprint, _ = fp.finish()

        # Store a hashed version of the fingerprint. Storing the hash
        # (instead of the raw chromaprint string) keeps the DB compact
        # and is sufficient for equality/index checks.
        return hashlib.sha1(fingerprint.encode()).hexdigest()

    except Exception as e:
        logging.debug(f"Fingerprint failed for {path}: {e}")
        return None

# ================= CLI =================

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--lib", required=True)
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--with-fingerprint", action="store_true",
        help="Compute Chromaprint fingerprint during ingest")
    args = parser.parse_args()
    # Call the main analysis routine. `--with-fingerprint` controls
    # whether chromaprint is used; it is passed explicitly to the
    # `analyze_files` function so the behavior is clear and testable.
    analyze_files(args.src, args.lib, args.progress, args.with_fingerprint)


if __name__ == "__main__":
    main()
