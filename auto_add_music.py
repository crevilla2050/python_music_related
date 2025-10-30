#!/usr/bin/env python3

# auto_add_music.py
# Watch a folder, auto-organize music into structured library,
# update DB if tags change, handle duplicates and missing tags.

import os
import sys
import time
import json
import re
import shutil
import sqlite3
import logging
import unicodedata
from hashlib import sha256
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from mutagen import File as MutagenFile
from mutagen.easyid3 import EasyID3
from musicbrainzngs import set_useragent, search_recordings
from organize_music import organize_file  # external dependency; must exist

# ---------------- CONFIG ----------------
WATCH_FOLDER = os.path.expanduser("~/Music/AutoAdd")
FAILED_FOLDER = os.path.expanduser("~/Music/FailedMusic")
DEFAULT_LIBRARY_ROOT = "/media/carlos/Asterix/MusicaCons"
DB_PATH = os.path.expanduser("~/Music/auto_add_music.db")
SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']
DUP_FUZZY_THRESHOLD = 87  # For artist/title comparison
# ---------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
set_useragent("MusicOrganizer", "1.0", "yourmail@example.com")

LIBRARY_ROOT = os.path.abspath(sys.argv[1]) if len(sys.argv) >= 2 else os.path.abspath(DEFAULT_LIBRARY_ROOT)
alias_file = sys.argv[2] if len(sys.argv) >= 3 else None

# Load aliases if provided
artist_aliases, album_aliases = {}, {}
if alias_file:
    try:
        with open(alias_file, "r", encoding="utf-8") as f:
            aliases = json.load(f)
            artist_aliases = aliases.get("artist_aliases", {})
            album_aliases = aliases.get("album_aliases", {})
        logging.info("Loaded alias file: %s", alias_file)
    except Exception as e:
        logging.warning("Failed to load alias file '%s': %s", alias_file, e)

# Ensure folders exist
os.makedirs(WATCH_FOLDER, exist_ok=True)
os.makedirs(LIBRARY_ROOT, exist_ok=True)
os.makedirs(FAILED_FOLDER, exist_ok=True)
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)

# Initialize DB (one-off)
_conn = sqlite3.connect(DB_PATH)
_cur = _conn.cursor()
_cur.execute("""
CREATE TABLE IF NOT EXISTS files (
    path TEXT PRIMARY KEY,
    artist TEXT,
    album TEXT,
    title TEXT,
    track TEXT,
    hash TEXT
)
""")
_conn.commit()
_cur.close()
_conn.close()

# ---------------- HELPERS ----------------
def normalize_string(s):
    if not s:
        return ""
    s = str(s)
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r'\s+', ' ', s).strip()
    return s

def file_hash(filepath):
    try:
        h = sha256()
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(65536), b''):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None

def normalize_filename(name, track_number=None):
    name_only, ext = os.path.splitext(name)
    name_only = re.sub(r'[\\/:*?"<>|]', '', name_only).strip()
    if track_number:
        tn = str(track_number).split('/')[0].strip()
        if tn.isdigit():
            name_only = f"{tn.zfill(2)}. {name_only}"
    return f"{name_only}{ext}"

def move_to_failed(filepath):
    try:
        base = os.path.basename(filepath)
        failed_path = os.path.join(FAILED_FOLDER, base)
        if os.path.exists(failed_path):
            ts = datetime.now().strftime("%Y%m%d-%H%M%S")
            name, ext = os.path.splitext(base)
            failed_path = os.path.join(FAILED_FOLDER, f"{name}-{ts}{ext}")
        shutil.move(filepath, failed_path)
        logging.info("Moved to Failed folder: %s", failed_path)
    except Exception as e:
        logging.warning("Failed to move to Failed folder: %s", e)

def musicbrainz_fallback(filepath):
    # Best-effort placeholder: real implementation should parse filename or audio fingerprint
    return None

# ---------------- PROCESSING ----------------
def is_duplicate(artist, title, filepath):
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT path, artist, title FROM files")
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logging.warning("DB error during duplicate check: %s", e)
        return False

    n_artist = normalize_string(artist)
    n_title = normalize_string(title)
    for row_path, db_artist, db_title in rows:
        if row_path == filepath:
            continue
        if normalize_string(db_artist) == n_artist and normalize_string(db_title) == n_title:
            return True
    return False

def check_and_update_file(filepath):
    if not os.path.isfile(filepath):
        return
    ext = os.path.splitext(filepath)[1].lower()
    if ext not in SUPPORTED_EXTS:
        return

    tags = {}
    try:
        audio = MutagenFile(filepath, easy=True)
        tags = {
            'artist': audio.get('artist', [''])[0] if audio else '',
            'title': audio.get('title', [''])[0] if audio else '',
            'album': audio.get('album', [''])[0] if audio else '',
            'track': audio.get('tracknumber', [''])[0] if audio else ''
        }
    except Exception:
        tags = {}

    if not tags.get('artist') or not tags.get('title'):
        fb = musicbrainz_fallback(filepath)
        if fb:
            tags.update(fb)
            try:
                # try to write tags back
                audio = MutagenFile(filepath, easy=True)
                if audio:
                    for k, v in fb.items():
                        audio[k] = v
                    audio.save()
            except Exception:
                pass
        else:
            move_to_failed(filepath)
            return

    artist = artist_aliases.get(tags.get('artist', ''), tags.get('artist', ''))
    album = album_aliases.get(tags.get('album', ''), tags.get('album', ''))
    title = tags.get('title', '')
    track = tags.get('track', '')

    try:
        new_path = organize_file(filepath, LIBRARY_ROOT, artist_aliases, album_aliases)
    except Exception as e:
        logging.warning("organize_file failed for %s: %s", filepath, e)
        move_to_failed(filepath)
        return

    if not new_path:
        logging.warning("organize_file returned no destination for %s", filepath)
        move_to_failed(filepath)
        return

    dest_dir = os.path.dirname(new_path)
    os.makedirs(dest_dir, exist_ok=True)
    normalized_name = normalize_filename(os.path.basename(new_path), track)
    final_path = os.path.join(dest_dir, normalized_name)

    if (not os.path.exists(filepath)) and os.path.exists(final_path):
        src_present = False
    else:
        src_present = True

    if is_duplicate(artist, title, final_path):
        logging.info("Duplicate detected, moving original to failed: %s", filepath)
        if src_present:
            move_to_failed(filepath)
        return

    if src_present:
        try:
            if os.path.exists(final_path):
                ts = datetime.now().strftime("%Y%m%d-%H%M%S")
                name, ext = os.path.splitext(normalized_name)
                final_path = os.path.join(dest_dir, f"{name}-{ts}{ext}")
            shutil.move(filepath, final_path)
            logging.info("Moved to: %s", final_path)
        except Exception as e:
            logging.warning("Failed to move %s -> %s : %s", filepath, final_path, e)
            move_to_failed(filepath)
            return
    else:
        logging.info("File already moved by organizer: %s", final_path)

    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        file_hash_val = file_hash(final_path)
        cur.execute("""
            INSERT OR REPLACE INTO files(path, artist, album, title, track, hash)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (final_path, artist, album, title, track, file_hash_val))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logging.warning("Failed to update DB for %s: %s", final_path, e)

# ---------------- WATCHDOG ----------------
class MusicHandler(FileSystemEventHandler):
    def on_created(self, event):
        if event.is_directory:
            return
        time.sleep(1)
        try:
            check_and_update_file(event.src_path)
        except Exception as e:
            logging.exception("Error handling created event for %s: %s", event.src_path, e)

def process_existing_files():
    logging.info("Processing existing files in %s...", WATCH_FOLDER)
    for root, _, files in os.walk(WATCH_FOLDER):
        for f in files:
            filepath = os.path.join(root, f)
            try:
                prev_size = -1
                stable = 0
                for _ in range(5):
                    if not os.path.exists(filepath):
                        break
                    size = os.path.getsize(filepath)
                    if size == prev_size:
                        stable += 1
                    else:
                        stable = 0
                    prev_size = size
                    if stable >= 2:
                        break
                    time.sleep(0.5)
            except Exception:
                pass
            try:
                check_and_update_file(filepath)
            except Exception as e:
                logging.exception("Error processing existing file %s: %s", filepath, e)

# ---------------- MAIN ----------------
if __name__ == "__main__":
    process_existing_files()
    observer = Observer()
    observer.daemon = True
    observer.schedule(MusicHandler(), path=WATCH_FOLDER, recursive=True)
    observer.start()
    logging.info("Watching %s -> %s", WATCH_FOLDER, LIBRARY_ROOT)
    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        logging.info("Stopping observer...")
        observer.stop()
    observer.join()
    conn.close()
    print("Usage: python scan_sources_sqlite.py <source_directory>")
    sys.exit(1)