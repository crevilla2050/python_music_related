#!/usr/bin/env python3
"""
consolidate_music.py

Music consolidation pipeline (Layer 1 – Knowledge)

- File discovery
- Metadata extraction
- SHA-256 hashing
- OPTIONAL Chromaprint fingerprinting
- OPTIONAL album art discovery
- Recommended canonical paths
- SQLite staging DB
- Explicit execution intent via `actions` table
"""

import os
import io
import sqlite3
import hashlib
import subprocess
import re
import unicodedata
import logging
from datetime import datetime, timezone
from pathlib import Path

from mutagen import File as MutagenFile
from dotenv import load_dotenv
from PIL import Image

try:
    from tqdm import tqdm
except Exception:
    tqdm = None

try:
    import chromaprint
except Exception:
    chromaprint = None

# ================= I18N MESSAGE KEYS =================

MSG_DB_NOT_PROVIDED = "DB_NOT_PROVIDED"
MSG_LIB_NOT_PROVIDED = "LIB_NOT_PROVIDED"

MSG_FOUND_AUDIO_FILES = "FOUND_AUDIO_FILES"
MSG_ANALYSIS_COMPLETE = "ANALYSIS_COMPLETE"

MSG_SCHEMA_UPGRADE_ADD_COLUMN = "SCHEMA_UPGRADE_ADD_COLUMN"

MSG_LAUNCH_GENRE_NORMALIZATION = "LAUNCH_GENRE_NORMALIZATION"
MSG_GENRE_NORMALIZATION_FAILED = "GENRE_NORMALIZATION_FAILED"

MSG_START_ANALYSIS = "START_ANALYSIS"
MSG_SKIP_FILE_STATE = "SKIP_FILE_STATE"

MSG_ACTION_SEEDED = "ACTION_SEEDED"

MSG_ALBUM_ART_SCAN = "ALBUM_ART_SCAN"

# ================= CONFIG =================

SUPPORTED_EXTS = {".mp3", ".flac", ".wav", ".m4a", ".ogg", ".aac", ".opus"}
IMAGE_EXTS = {".jpg", ".jpeg", ".png"}
COMMON_COVER_NAMES = {"cover", "folder", "front", "album", "albumart"}

ENABLE_CHROMAPRINT = True
FP_SECONDS = 90
DATABASES_DIR = Path("databases")

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
)

# ================= ENV HELPERS =================

def _update_env(key, value):
    lines = []
    if os.path.exists(".env"):
        with open(".env", "r") as f:
            lines = f.readlines()

    lines = [l for l in lines if not l.startswith(f"{key}=")]
    lines.append(f"{key}={value}\n")

    with open(".env", "w") as f:
        f.writelines(lines)


def resolve_env_path(key, cli_value=None):
    """
    Resolve a path using:
    1. CLI argument (if provided)
    2. .env fallback
    """
    load_dotenv(override=False)

    if cli_value:
        value = os.path.abspath(cli_value)
        _update_env(key, value)
        return value

    value = os.getenv(key)
    if value:
        return os.path.abspath(value)

    raise RuntimeError({
        "key": MSG_DB_NOT_PROVIDED if key == "MUSIC_DB" else MSG_LIB_NOT_PROVIDED,
        "params": {"key": key}
    })

def resolve_database_path(cli_value=None):
    """
    Resolve database path with automatic ./databases/ placement.

    Priority:
    1. --db argument
    2. MUSIC_DB from .env

    If a filename (no directory) is provided, it is placed under ./databases/.
    """
    DATABASES_DIR.mkdir(exist_ok=True)

    load_dotenv(override=False)

    raw = cli_value or os.getenv("MUSIC_DB")
    if not raw:
        raise RuntimeError({
            "key": MSG_DB_NOT_PROVIDED,
            "params": {"key": "MUSIC_DB"}
        })

    p = Path(raw)

    # If no directory part, place inside ./databases
    if p.parent == Path("."):
        p = DATABASES_DIR / p

    p = p.resolve()

    _update_env("MUSIC_DB", str(p))
    return str(p)


# ================= UTILITIES =================

def log(msg):
    logging.info(msg)


def maybe_progress(it, desc=None, enable=False):
    if enable and tqdm:
        return tqdm(it, desc=desc)
    return it


def utcnow():
    return datetime.now(timezone.utc).isoformat()


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
    if not track_raw:
        return None
    if isinstance(track_raw, list):
        track_raw = track_raw[0]
    track_raw = str(track_raw).strip()
    if "/" in track_raw:
        track_raw = track_raw.split("/", 1)[0]
    return f"{int(track_raw):02d}" if track_raw.isdigit() else None


def recommended_path_for(root, meta, ext):
    artist = sanitize_for_fs(meta.get("album_artist") or meta.get("artist") or "Unknown Artist")
    album = sanitize_for_fs(meta.get("album") or "Unknown Album")
    title = sanitize_for_fs(meta.get("title") or meta.get("orig_name"))
    track = meta.get("track")
    fname = f"{track} - {title}{ext}" if track else f"{title}{ext}"
    return str(Path(root) / artist / album / fname)


def hash_image_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ================= ALBUM ART INGEST =================

def ingest_album_art_for_file(c, file_row, audio_path: Path):
    album_artist = file_row["album_artist"]
    album = file_row["album"]
    is_comp = file_row["is_compilation"]

    if not album:
        return

    # ----- Embedded art -----
    try:
        audio = MutagenFile(audio_path, easy=False)
        if audio and hasattr(audio, "tags"):
            for tag in audio.tags.values():
                if hasattr(tag, "data"):
                    data = tag.data
                    h = hash_image_bytes(data)

                    img = Image.open(io.BytesIO(data))
                    width, height = img.size

                    c.execute("""
                        INSERT OR IGNORE INTO album_art (
                            album_artist, album, is_compilation,
                            image_hash, source, confidence,
                            mime, width, height, created_at
                        )
                        VALUES (?, ?, ?, ?, 'embedded', 0.9, ?, ?, ?, ?)
                    """, (
                        album_artist, album, is_comp,
                        h, img.format, width, height, utcnow()
                    ))
    except Exception:
        pass

    # ----- External art -----
    try:
        for p in audio_path.parent.iterdir():
            # ----- External art (ALL images in album directory) -----
            try:
                for p in audio_path.parent.iterdir():
                    if not p.is_file():
                        continue
                    if p.suffix.lower() not in IMAGE_EXTS:
                        continue

                    # Include hidden files as well (.cover.jpg, etc.)
                    data = p.read_bytes()
                    h = hash_image_bytes(data)

                    img = Image.open(p)
                    width, height = img.size

                    c.execute("""
                        INSERT OR IGNORE INTO album_art (
                            album_artist, album, is_compilation,
                            image_hash, source, confidence,
                            mime, width, height, created_at
                        )
                        VALUES (?, ?, ?, ?, 'external', 0.5, ?, ?, ?, ?)
                    """, (
                        album_artist, album, is_comp,
                        h, img.format, width, height, utcnow()
                    ))
            except Exception:
                pass

            data = p.read_bytes()
            h = hash_image_bytes(data)

            img = Image.open(p)
            width, height = img.size

            c.execute("""
                INSERT OR IGNORE INTO album_art (
                    album_artist, album, is_compilation,
                    image_hash, source, confidence,
                    mime, width, height, created_at
                )
                VALUES (?, ?, ?, ?, 'external', 0.8, ?, ?, ?, ?)
            """, (
                album_artist, album, is_comp,
                h, img.format, width, height, utcnow()
            ))
    except Exception:
        pass


# ================= METADATA =================

def extract_tags(path: Path):
    try:
        audio = MutagenFile(path, easy=True)
        raw = MutagenFile(path, easy=False)

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
            "genre": audio.get("genre", [None])[0],
            "duration": getattr(audio.info, "length", None),
            "bitrate": getattr(audio.info, "bitrate", None),
            "is_compilation": is_comp,
            "orig_name": path.stem,
        }
    except Exception:
        return {
            "artist": None,
            "album_artist": None,
            "album": None,
            "title": None,
            "track": None,
            "genre": None,
            "duration": None,
            "bitrate": None,
            "is_compilation": 0,
            "orig_name": path.stem,
        }


# ================= DATABASE =================

def create_db(db_path):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
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
        genre TEXT,
        duration REAL,
        bitrate INTEGER,
        fingerprint TEXT,
        is_compilation INTEGER DEFAULT 0,
        recommended_path TEXT,
        lifecycle_state TEXT DEFAULT 'new',
        first_seen TEXT,
        last_update TEXT,
        notes TEXT
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
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS actions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        file_id INTEGER NOT NULL,
        action TEXT NOT NULL,
        src_path TEXT NOT NULL,
        dst_path TEXT,
        status TEXT DEFAULT 'pending',
        error TEXT,
        created_at TEXT NOT NULL,
        applied_at TEXT,
        FOREIGN KEY(file_id) REFERENCES files(id)
    );

    CREATE TABLE IF NOT EXISTS genres (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE,
        normalized_name TEXT NOT NULL UNIQUE,
        source TEXT DEFAULT 'user',
        created_at TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS genre_mappings (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        raw_token TEXT NOT NULL,
        normalized_token TEXT NOT NULL UNIQUE,
        genre_id INTEGER,
        source TEXT DEFAULT 'user',
        created_at TEXT NOT NULL,
        FOREIGN KEY (genre_id) REFERENCES genres(id)
    );

    CREATE TABLE IF NOT EXISTS file_genres (
        file_id INTEGER NOT NULL,
        genre_id INTEGER NOT NULL,
        source TEXT DEFAULT 'tag',
        confidence REAL DEFAULT 1.0,
        created_at TEXT NOT NULL,
        PRIMARY KEY (file_id, genre_id),
        FOREIGN KEY (file_id) REFERENCES files(id),
        FOREIGN KEY (genre_id) REFERENCES genres(id)
    );

    CREATE INDEX IF NOT EXISTS idx_genres_norm
        ON genres(normalized_name);
    CREATE INDEX IF NOT EXISTS idx_file_genres_file
        ON file_genres(file_id);
    CREATE INDEX IF NOT EXISTS idx_file_genres_genre
        ON file_genres(genre_id);
    CREATE INDEX IF NOT EXISTS idx_genre_mappings_norm
        ON genre_mappings(normalized_token);
    """)

    # ---- schema migration (safe) ----
    def ensure_column(table, column, ddl):
        cols = [r["name"] for r in c.execute(f"PRAGMA table_info({table})")]
        if column not in cols:
            log({
                "key": MSG_SCHEMA_UPGRADE_ADD_COLUMN,
                "params": {"table": table, "column": column}
            })
            c.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

    ensure_column(
        "files",
        "lifecycle_state",
        "lifecycle_state TEXT NOT NULL DEFAULT 'new'"
    )

    conn.commit()
    return conn


    # ---------------- schema migration ----------------


# ================= FINGERPRINT =================

def compute_fingerprint(path: Path):
    if not ENABLE_CHROMAPRINT or chromaprint is None:
        return None

    try:
        cmd = [
            "ffmpeg", "-v", "quiet",
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

        if not proc.stdout:
            return None

        fp = chromaprint.Fingerprinter(44100, 1)
        fp.feed(proc.stdout)
        fingerprint, _ = fp.finish()

        return hashlib.sha1(fingerprint.encode()).hexdigest()

    except Exception:
        return None


# ================= INGEST =================

def analyze_files(
    src,
    lib,
    db_path,
    progress=False,
    with_fingerprint=False,
    search_covers=False,
    only_states=None,
    exclude_states=None,
):
    conn = create_db(db_path)
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    audio_list = [p for p in Path(src).rglob("*") if is_audio_file(p)]
    log({
        "key": MSG_FOUND_AUDIO_FILES,
        "params": {"count": len(audio_list)}
    })

    for p in maybe_progress(audio_list, "Analyzing", progress):
        meta = extract_tags(p)
        sha = sha256_file(p)
        fp = compute_fingerprint(p) if with_fingerprint else None
        rec = recommended_path_for(lib, meta, p.suffix)
        now = utcnow()

        # ------------------------------------------------------------
        # Check if file already exists
        # ------------------------------------------------------------
        row = c.execute(
            "SELECT id, lifecycle_state FROM files WHERE original_path=?",
            (str(p),)
        ).fetchone()

        if row:
            lifecycle = row["lifecycle_state"]

            # --only-state filter
            if only_states and lifecycle not in only_states:
                continue

            # --exclude-state filter
            if exclude_states and lifecycle in exclude_states:
                continue

            is_new = False
        else:
            lifecycle = "new"
            is_new = True

        # ------------------------------------------------------------
        # UPSERT factual data (NEVER overwrites user intent)
        # ------------------------------------------------------------
        c.execute("""
            INSERT INTO files (
                original_path, sha256, size_bytes,
                artist, album_artist, album, title, track, genre,
                duration, bitrate, fingerprint,
                is_compilation, recommended_path,
                lifecycle_state,
                first_seen, last_update
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(original_path) DO UPDATE SET
                sha256=excluded.sha256,
                size_bytes=excluded.size_bytes,
                artist=excluded.artist,
                album_artist=excluded.album_artist,
                album=excluded.album,
                title=excluded.title,
                track=excluded.track,
                genre=excluded.genre,
                duration=excluded.duration,
                bitrate=excluded.bitrate,
                fingerprint=excluded.fingerprint,
                is_compilation=excluded.is_compilation,
                recommended_path=excluded.recommended_path,
                last_update=excluded.last_update
        """, (
            str(p), sha, p.stat().st_size,
            meta["artist"], meta["album_artist"],
            meta["album"], meta["title"], meta["track"],
            meta.get("genre"),
            meta["duration"], meta["bitrate"], fp,
            meta["is_compilation"], rec,
            lifecycle,
            now, now
        ))

        file_id = c.execute(
            "SELECT id FROM files WHERE original_path=?",
            (str(p),)
        ).fetchone()[0]

        # ------------------------------------------------------------
        # Seed initial action ONLY for new files
        # ------------------------------------------------------------
        if is_new:
            c.execute("""
                INSERT INTO actions (
                    file_id, action, src_path, dst_path, created_at
                )
                VALUES (?, 'move', ?, ?, ?)
            """, (file_id, str(p), rec, now))

        # ------------------------------------------------------------
        # Album art discovery (knowledge only)
        # ------------------------------------------------------------
        if search_covers:
            file_row = c.execute("""
                SELECT album_artist, album, is_compilation
                FROM files WHERE id=?
            """, (file_id,)).fetchone()

            ingest_album_art_for_file(c, file_row, p)

    conn.commit()
    conn.close()
    log(MSG_ANALYSIS_COMPLETE)


# ================= CLI =================

def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--src", required=True)
    parser.add_argument("--lib")
    parser.add_argument("--db")
    parser.add_argument("--progress", action="store_true")
    parser.add_argument("--with-fingerprint", action="store_true")
    parser.add_argument(
        "--search-covers",
        action="store_true",
        help="Discover embedded and external album art (Layer 1 knowledge)"
    )
    parser.add_argument(
        "--edit-tags",
        action="store_true",
        help="Launch interactive genre normalization after ingest"
    )
    parser.add_argument(
        "--only-state",
        help="Comma-separated lifecycle states to include (e.g. new,reviewing)"
    )
    parser.add_argument(
        "--exclude-state",
        help="Comma-separated lifecycle states to exclude (e.g. applied,locked)"
    )

    args = parser.parse_args()

    # Resolve paths
    db_path = resolve_database_path(args.db)
    lib_path = resolve_env_path("MUSIC_LIB", args.lib)

    # Parse lifecycle filters
    only_states = None
    exclude_states = None

    if args.only_state:
        only_states = {s.strip() for s in args.only_state.split(",") if s.strip()}

    if args.exclude_state:
        exclude_states = {s.strip() for s in args.exclude_state.split(",") if s.strip()}

    analyze_files(
        src=args.src,
        lib=lib_path,
        db_path=db_path,
        progress=args.progress,
        with_fingerprint=args.with_fingerprint,
        search_covers=args.search_covers,
        only_states=only_states,
        exclude_states=exclude_states,
    )

    # ------------------------------------------------------------
    # Optional interactive tag normalization
    # ------------------------------------------------------------
    if args.edit_tags:
        log(MSG_LAUNCH_GENRE_NORMALIZATION)
        try:
            from genre_normalizer_cli import main as genre_cli_main
            genre_cli_main(db_path=db_path)
        except Exception as e:
            log({
                "key": MSG_GENRE_NORMALIZATION_FAILED,
                "params": {"error": str(e)}
            })


if __name__ == "__main__":
    main()
