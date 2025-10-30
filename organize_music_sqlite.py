#!/usr/bin/env python3
# organize_music_sqlite.py

import os
import sys
import sqlite3
import subprocess
import hashlib
import time
import re
import unicodedata
from mutagen import File as MutagenFile
from difflib import SequenceMatcher

# ------------------ CONFIG ------------------
SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']
DB_FILE = "music_library.db"
MAX_DIRNAME_LEN = 100
MAX_FILENAME_LEN = 100
DUP_SUFFIX = "_dup"
FUZZY_THRESHOLD = 0.85  # for artist/title fuzzy match
FP_BLOCK = 65536
# --------------------------------------------

# ------------------ LOGGING -----------------
def log(msg):
    print(msg)
# --------------------------------------------

# ------------------ DB ----------------------
def init_db(db_file):
    conn = sqlite3.connect(db_file)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            path TEXT NOT NULL,
            artist TEXT,
            album TEXT,
            title TEXT,
            track TEXT,
            hash_fp TEXT,
            file_mtime INTEGER,
            first_seen INTEGER,
            status TEXT
        )
    ''')
    c.execute('CREATE INDEX IF NOT EXISTS idx_hash_fp ON files(hash_fp)')
    c.execute('CREATE INDEX IF NOT EXISTS idx_path ON files(path)')
    conn.commit()
    return conn
# --------------------------------------------

# ----------------- UTILITIES ----------------
def normalize_dirname(name):
    name = unicodedata.normalize("NFKD", name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    name = re.sub(r'\s*\.+\s*', '_', name)
    name = name.strip(' .')
    name = ''.join(c for c in name if c.isalnum() or c in " _-().[]")
    return name[:MAX_DIRNAME_LEN]

def normalize_filename(name, track=None):
    name_only, ext = os.path.splitext(name)
    name_only = re.sub(r'[\\/:*?"<>|]', '', name_only).strip()
    if track and track.isdigit():
        name_only = f"{track.zfill(2)}. {name_only}"
    return f"{name_only}{ext}"

def compute_sha1_fingerprint(fp_file):
    return hashlib.sha1(fp_file.encode('utf-8')).hexdigest()

def compute_fingerprint(path, max_retries=2, length=120):
    """
    Run fpcalc and return SHA-1 of fingerprint.
    Retries on failure, reduces length on repeated crashes.
    Returns None if all attempts fail.
    """
    attempt = 0
    while attempt <= max_retries:
        try:
            cmd_length = max(length - attempt*30, 30)  # reduce length on retry
            result = subprocess.run(
                ['fpcalc', '-raw', '-length', str(cmd_length), path],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True,
                check=True
            )
            for line in result.stdout.splitlines():
                if line.startswith("FINGERPRINT="):
                    fp_raw = line.split("=", 1)[1]
                    return compute_sha1_fingerprint(fp_raw)
            log(f"[!] fpcalc returned no fingerprint: {path}")
            return None
        except subprocess.CalledProcessError as e:
            log(f"[!] fpcalc failed (attempt {attempt+1}/{max_retries+1}): {path} -> {e}")
        except Exception as e:
            log(f"[!] Unexpected error running fpcalc (attempt {attempt+1}/{max_retries+1}): {path} -> {e}")
        attempt += 1
    log(f"[!] All fpcalc attempts failed: {path}")
    return None
# --------------------------------------------

def extract_tags(filepath):
    """Extract tags from audio file, fallback to filename parsing"""
    try:
        audio = MutagenFile(filepath, easy=True)
        artist = album = title = track = ""

        if audio:
            artist = audio.get("artist", [""])[0].strip()
            album  = audio.get("album", [""])[0].strip()
            title  = audio.get("title", [""])[0].strip()
            track  = audio.get("tracknumber", [""])[0].split("/")[0].strip()

        # Fallback: parse filename
        if not artist or not title:
            fname = os.path.splitext(os.path.basename(filepath))[0]
            if " - " in fname:
                parts = fname.split(" - ", 1)
                if not artist:
                    artist = parts[0].strip()
                if not title:
                    title = parts[1].strip()
            elif "_" in fname:
                parts = fname.split("_")
                if not artist:
                    artist = parts[0].strip()
                if not title:
                    title = "_".join(parts[1:]).strip()

        if not artist:
            artist = "Unknown Artist"
        if not album:
            album = "Unknown Album"
        if not title:
            title = os.path.splitext(os.path.basename(filepath))[0].strip()
        if not track:
            track = ""

        return {"artist": artist, "album": album, "title": title, "track": track}
    except Exception as e:
        log(f"[!] Error extracting tags from '{filepath}': {e}")
        fname = os.path.splitext(os.path.basename(filepath))[0]
        return {"artist": "Unknown Artist", "album": "Unknown Album", "title": fname, "track": ""}

def fuzzy_match_tags(t1, t2):
    artist1, title1 = t1
    artist2, title2 = t2
    artist_ratio = SequenceMatcher(None, artist1.lower(), artist2.lower()).ratio()
    title_ratio = SequenceMatcher(None, title1.lower(), title2.lower()).ratio()
    return artist_ratio > FUZZY_THRESHOLD and title_ratio > FUZZY_THRESHOLD

def get_unique_dest(dest_path):
    base, ext = os.path.splitext(dest_path)
    counter = 1
    while os.path.exists(dest_path):
        dest_path = f"{base}{DUP_SUFFIX}{counter}{ext}"
        counter += 1
    return dest_path
# --------------------------------------------

# -------------- CORE LOGIC ------------------
def organize_file(conn, path, target_root):
    if not os.path.isfile(path):
        return

    tags = extract_tags(path)
    artist_dir = normalize_dirname(tags['artist'])
    album_dir = normalize_dirname(tags['album'])
    dest_dir = os.path.join(target_root, artist_dir, album_dir)
    os.makedirs(dest_dir, exist_ok=True)

    filename = normalize_filename(f"{tags['artist']} - {tags['title']}{os.path.splitext(path)[1]}", tags['track'])
    dest_path = os.path.join(dest_dir, filename)

    # Compute fingerprint
    hash_fp = compute_fingerprint(path)

    # Move file first (regardless of fingerprint)
    if os.path.abspath(path) != os.path.abspath(dest_path):
        os.rename(path, dest_path)
        log(f"[✓] Moved {path} -> {dest_path}")
        path_to_store = dest_path
    else:
        path_to_store = path

    c = conn.cursor()
    status = "active"

    # Check for duplicates by fingerprint
    if hash_fp:
        c.execute("SELECT id, path, status FROM files WHERE hash_fp=?", (hash_fp,))
        row = c.fetchone()
        if row:
            orig_path = row[1]
            new_dest = get_unique_dest(dest_path)
            os.rename(path_to_store, new_dest)
            log(f"[DUPLICATE] {path_to_store} -> {new_dest}")
            path_to_store = new_dest
            status = "duplicate"
    else:
        # Fuzzy duplicate fallback
        c.execute("SELECT artist, title, path FROM files WHERE status='active'")
        for r in c.fetchall():
            if fuzzy_match_tags((tags['artist'], tags['title']), (r[0], r[1])):
                status = "suspected_duplicate"
                log(f"[SUSPECTED DUPLICATE] {path_to_store} ~ {r[2]}")
                break

    # Insert or update DB
    c.execute("SELECT id, hash_fp, file_mtime, status FROM files WHERE path=?", (path_to_store,))
    existing = c.fetchone()
    now = int(time.time())
    mtime = int(os.path.getmtime(path_to_store))

    if existing:
        db_hash_fp, db_mtime, db_status = existing[1], existing[2], existing[3]
        if db_mtime != mtime or db_hash_fp != hash_fp or db_status != status:
            c.execute("""
                UPDATE files SET artist=?, album=?, title=?, track=?, hash_fp=?, file_mtime=?, status=? WHERE path=?
            """, (tags['artist'], tags['album'], tags['title'], tags['track'], hash_fp, mtime, status, path_to_store))
            log(f"[DB UPDATED] {path_to_store}")
    else:
        c.execute("""
            INSERT INTO files (path, artist, album, title, track, hash_fp, file_mtime, first_seen, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path_to_store, tags['artist'], tags['album'], tags['title'], tags['track'], hash_fp, mtime, now, status))
        log(f"[DB INSERT] {path_to_store}")

    conn.commit()

def process_directory(source_dir, target_dir):
    conn = init_db(DB_FILE)
    for root, _, files in os.walk(source_dir):
        for name in files:
            ext = os.path.splitext(name)[1].lower()
            if ext not in SUPPORTED_EXTS:
                continue
            full_path = os.path.join(root, name)
            organize_file(conn, full_path, target_dir)
    conn.close()
# --------------------------------------------

# ----------------- MAIN --------------------
if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python organize_music_sqlite.py <source_dir> <target_dir>")
        sys.exit(1)

    source_dir = sys.argv[1]
    target_dir = sys.argv[2]

    process_directory(source_dir, target_dir)
    print("\n[✔] Music library organization complete.\n")
# --------------------------------------------
