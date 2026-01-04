#!/usr/bin/env python3
"""
consolidate_music.py

Music consolidation pipeline with:
- SHA-256 identity
- Canonical resolution
- OPTIONAL Chromaprint fingerprinting
- Fuzzy filename + metadata similarity
- Safe staged decisions via SQLite
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

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import chromaprint
except Exception:
    chromaprint = None

# ================= CONFIG =================

DB_PATH = "music_consolidation.db"
PLAN_JSON = "move_plan.json"

SUPPORTED_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".aac", ".opus"}

ENABLE_CHROMAPRINT = True
FP_SECONDS = 90

DUP_FILENAME_THRESHOLD = 85
METADATA_SIMILARITY_THRESHOLD = 70

METADATA_ARTIST_WEIGHT = 0.45
METADATA_TITLE_WEIGHT = 0.45
METADATA_DURATION_BONUS = 10
METADATA_BITRATE_BONUS = 5

DURATION_TOLERANCE_SECONDS = 3.0
BITRATE_REL_TOLERANCE = 0.20

# ========================================


def log(msg):
    logging.info(msg)


logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)


def maybe_progress(it, desc=None, enable=False):
    if enable and tqdm:
        return tqdm(it, desc=desc)
    return it


# ================= DATABASE =================

def init_db():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.executescript("""
    PRAGMA foreign_keys = ON;

    CREATE TABLE IF NOT EXISTS files (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        original_path TEXT UNIQUE,
        sha256 TEXT,
        size_bytes INTEGER,
        artist TEXT,
        album TEXT,
        title TEXT,
        track TEXT,
        duration REAL,
        bitrate INTEGER,
        fingerprint TEXT,
        status TEXT DEFAULT 'pending',
        action TEXT NOT NULL DEFAULT 'move',
        recommended_path TEXT,
        first_seen TEXT,
        last_update TEXT,
        notes TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_files_sha ON files(sha256);
    CREATE INDEX IF NOT EXISTS idx_files_fp ON files(fingerprint);
    CREATE INDEX IF NOT EXISTS idx_files_status_action ON files(status, action);
    """)
    conn.commit()
    return conn


# ================= UTILITIES =================

def is_audio_file(p: Path):
    return p.is_file() and p.suffix.lower() in SUPPORTED_EXTS


def sha256_file(path: Path):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def normalize_str(s):
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c)).strip()


def sanitize_for_fs(s):
    if not s:
        return "Unknown"
    s = normalize_str(s)
    s = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "_", s)
    return s.strip(" .")[:120]


def normalize_track(track_raw):
    """
    Normalize track numbers to 2-digit strings.
    Examples:
      "3"     -> "03"
      "03"    -> "03"
      "3/12"  -> "03"
      None    -> None
      "abc"   -> None
    """
    if not track_raw:
        return None

    if isinstance(track_raw, list):
        track_raw = track_raw[0]

    track_raw = str(track_raw).strip()

    if "/" in track_raw:
        track_raw = track_raw.split("/", 1)[0]

    if not track_raw.isdigit():
        return None

    num = int(track_raw)
    if num <= 0 or num > 99:
        return None

    return f"{num:02d}"


def recommended_path_for(root, meta, ext):
    artist = sanitize_for_fs(meta.get("artist") or "Unknown Artist")
    album = sanitize_for_fs(meta.get("album") or "Unknown Album")

    raw_title = meta.get("title")
    if raw_title:
        title = sanitize_for_fs(raw_title)
    else:
        title = sanitize_for_fs(Path(meta.get("orig_name", "Track")).stem)

    track = meta.get("track")

    if track:
        fname = f"{track} - {title}{ext}"
    else:
        fname = f"{title}{ext}"

    return str(Path(root) / artist / album / fname)


# ================= METADATA =================

def extract_tags(path: Path):
    try:
        audio = MutagenFile(path, easy=True)
        if not audio:
            raise Exception("Unsupported")

        raw_track = audio.get("tracknumber", [None])[0]
        norm_track = normalize_track(raw_track)

        return {
            "artist": audio.get("artist", [None])[0],
            "album": audio.get("album", [None])[0],
            "title": audio.get("title", [None])[0],
            "track": norm_track,
            "duration": getattr(audio.info, "length", None),
            "bitrate": getattr(audio.info, "bitrate", None),
            "orig_name": path.name
        }
    except Exception:
        return {
            "artist": None,
            "album": None,
            "title": None,
            "track": None,
            "duration": None,
            "bitrate": None,
            "orig_name": path.name
        }


# ================= FINGERPRINT =================

def compute_fingerprint(path: Path):
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
        fp = chromaprint.Fingerprinter(44100, 1)
        fp.feed(pcm)
        fingerprint, _ = fp.finish()

        return hashlib.sha1(fingerprint.encode()).hexdigest()

    except Exception:
        logging.debug("Fingerprinting failed for %s", path, exc_info=True)
        return None


# ================= DUPLICATE SCORING =================

def filename_fuzzy_score(p1: Path, p2: Path):
    def norm(p):
        s = re.sub(r'^\d+\s*[-._]\s*', '', p.stem)
        s = re.sub(r'[\[\]\(\)\-_.]', ' ', s)
        s = unicodedata.normalize("NFKD", s).casefold()
        return re.sub(r'\s+', ' ', s).strip()
    return fuzz.ratio(norm(p1), norm(p2))


def metadata_similarity_score(m1, m2):
    a = fuzz.ratio(normalize_str(m1.get("artist")), normalize_str(m2.get("artist")))
    t = fuzz.ratio(normalize_str(m1.get("title")), normalize_str(m2.get("title")))
    score = a * METADATA_ARTIST_WEIGHT + t * METADATA_TITLE_WEIGHT

    if m1.get("duration") and m2.get("duration"):
        if abs(m1["duration"] - m2["duration"]) <= DURATION_TOLERANCE_SECONDS:
            score += METADATA_DURATION_BONUS

    if m1.get("bitrate") and m2.get("bitrate"):
        if abs(m1["bitrate"] - m2["bitrate"]) / max(1, m1["bitrate"]) <= BITRATE_REL_TOLERANCE:
            score += METADATA_BITRATE_BONUS

    return min(100.0, score)


# ================= UPSERT =================

def upsert_file(conn, path, sha, size, meta, rec, fp):
    now = datetime.now(timezone.utc).isoformat()
    c = conn.cursor()
    c.execute("""
        INSERT INTO files (
            original_path, sha256, size_bytes,
            artist, album, title, track,
            duration, bitrate, fingerprint,
            recommended_path, first_seen, last_update
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(original_path) DO UPDATE SET
            sha256=excluded.sha256,
            size_bytes=excluded.size_bytes,
            artist=excluded.artist,
            album=excluded.album,
            title=excluded.title,
            track=excluded.track,
            duration=excluded.duration,
            bitrate=excluded.bitrate,
            fingerprint=excluded.fingerprint,
            recommended_path=excluded.recommended_path,
            last_update=excluded.last_update
    """, (
        str(path), sha, size,
        meta["artist"], meta["album"], meta["title"], meta["track"],
        meta["duration"], meta["bitrate"], fp,
        rec, now, now
    ))


# ================= CANONICAL RESOLUTION =================

def resolve_fingerprint_duplicates(conn):
    c = conn.cursor()
    c.execute("""
        SELECT fingerprint FROM files
        WHERE fingerprint IS NOT NULL
        GROUP BY fingerprint
        HAVING COUNT(*) > 1
    """)
    fps = [r[0] for r in c.fetchall()]

    log(f"Resolving {len(fps)} fingerprint clusters")

    for fp in fps:
        c.execute("""
            SELECT id, artist, album, title, bitrate
            FROM files WHERE fingerprint=?
        """, (fp,))
        rows = c.fetchall()

        rows.sort(
            key=lambda r: (
                bool(r[1]) + bool(r[2]) + bool(r[3]),
                r[4] or 0
            ),
            reverse=True
        )

        winner = rows[0][0]
        c.execute("UPDATE files SET action='move', status='unique' WHERE id=?", (winner,))

        for r in rows[1:]:
            c.execute("""
                UPDATE files
                SET action='archive',
                    status='duplicate',
                    notes='fingerprint_match'
                WHERE id=?
            """, (r[0],))

    conn.commit()


# ================= ANALYSIS =================

def analyze_files(src, lib, progress=False):
    conn = init_db()

    audio_list = [p for p in Path(src).rglob("**/*") if is_audio_file(p)]
    log(f"Found {len(audio_list)} audio files")

    try:
        with conn:
            for p in maybe_progress(audio_list, "Analyzing", progress):
                try:
                    sha = sha256_file(p)
                    meta = extract_tags(p)
                    fp = compute_fingerprint(p)
                    rec = recommended_path_for(lib, meta, p.suffix)
                    upsert_file(conn, p, sha, p.stat().st_size, meta, rec, fp)
                except Exception:
                    logging.debug("Failed to process %s", p, exc_info=True)

        resolve_fingerprint_duplicates(conn)
        build_plan_json(conn)

    finally:
        conn.close()

    log("Analysis complete")


# ================= JSON =================

def build_plan_json(conn):
    c = conn.cursor()
    c.execute("SELECT * FROM files")
    cols = [d[0] for d in c.description]
    files = [dict(zip(cols, r)) for r in c.fetchall()]

    with open(PLAN_JSON, "w", encoding="utf-8") as f:
        json.dump(
            {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "files": files
            },
            f,
            indent=2
        )

    log(f"Wrote {PLAN_JSON}")


# ================= CLI =================

def main():
    parser = __import__("argparse").ArgumentParser(description="Consolidate music library")
    parser.add_argument("--src")
    parser.add_argument("--lib")
    parser.add_argument("--analyze-files", nargs=2, metavar=("SRC", "LIB"))
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--db")
    parser.add_argument("--plan")

    args = parser.parse_args()

    if args.analyze_files:
        src, lib = args.analyze_files
    else:
        src, lib = args.src, args.lib

    if not src or not lib:
        parser.print_help()
        return

    global DB_PATH, PLAN_JSON
    if args.db:
        DB_PATH = args.db
    if args.plan:
        PLAN_JSON = args.plan

    analyze_files(src, lib, args.progress)


if __name__ == "__main__":
    main()
