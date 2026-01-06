#!/usr/bin/env python3
# organize_music_sqlite.py
# DEPRECATED — superseded by consolidate_music.py + execute_actions.py

import os
import sys
import sqlite3
import subprocess
import hashlib
import time
import re
import unicodedata
import chromaprint
import shutil
import tempfile
from mutagen import File as MutagenFile
from mutagen.easyid3 import EasyID3
from difflib import SequenceMatcher

# ------------------ CONFIG ------------------
SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']
DB_FILE = "music_library.db"
MAX_DIRNAME_LEN = 100
MAX_FILENAME_LEN = 100
DUP_SUFFIX = "_dup"
FUZZY_THRESHOLD = 0.85  # for artist/title fuzzy match
# Fingerprint settings
FP_MAX_SECONDS = 120
FP_MIN_SECONDS = 30
FP_RETRIES = 3
# Re-encode behavior
REENCODE_ON_FAILURE = True  # toggle re-encoding attempts
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
# --------------------------------------------

# ---------- Re-encoding helpers -------------
def probe_format(path):
    """
    Return lowercased extension without dot (mp3, flac, wav, m4a, ogg, aac).
    If unknown, return None.
    """
    ext = os.path.splitext(path)[1].lower().lstrip('.')
    if ext in ['mp3', 'flac', 'wav', 'm4a', 'ogg', 'aac']:
        return ext
    return None

def read_tags_generic(path):
    """Return a dict with artist, album, title, track to reapply after re-encode."""
    try:
        audio = MutagenFile(path, easy=True)
        if not audio:
            return {}
        return {
            'artist': audio.get('artist', [''])[0],
            'album': audio.get('album', [''])[0],
            'title': audio.get('title', [''])[0],
            'tracknumber': audio.get('tracknumber', [''])[0]
        }
    except Exception:
        return {}

def write_tags_generic(path, tags):
    """Attempt to write simple tags to the new file using Mutagen."""
    try:
        audio = MutagenFile(path, easy=True)
        if audio is None:
            return
        for k, v in tags.items():
            if not v:
                continue
            # Map tracknumber key to tag name used by Mutagen
            if k == 'tracknumber':
                audio['tracknumber'] = v
            else:
                audio[k] = v
        audio.save()
    except Exception as e:
        log(f"[!] Failed to write tags to {path}: {e}")

def reencode_file_same_format(src_path):
    """
    Re-encode src_path into a temporary file using same codec/format with high-quality settings.
    Returns temp_path or None on failure.
    """
    fmt = probe_format(src_path)
    if not fmt:
        log(f"[!] Unknown format for re-encode: {src_path}")
        return None

    # prepare temp output path
    fd, tmp_path = tempfile.mkstemp(suffix='.' + fmt)
    os.close(fd)  # we'll let ffmpeg write it
    tags = read_tags_generic(src_path)

    # choose ffmpeg args per format (high-quality / lossless where possible)
    # We keep channels/sample rate untouched except when needed to ensure compatibility.
    if fmt == 'mp3':
        # Use libmp3lame, high quality VBR (~320kbps)
        args = ['-c:a', 'libmp3lame', '-qscale:a', '0']
    elif fmt == 'flac':
        # FLAC lossless, compression level 5 (balanced)
        args = ['-c:a', 'flac', '-compression_level', '5']
    elif fmt == 'wav':
        # PCM 16-bit, 44.1k
        args = ['-c:a', 'pcm_s16le']
    elif fmt in ('m4a', 'aac'):
        # AAC via native encoder - high bitrate
        args = ['-c:a', 'aac', '-b:a', '256k']
    elif fmt == 'ogg':
        # Vorbis with quality 6
        args = ['-c:a', 'libvorbis', '-qscale:a', '6']
    elif fmt == 'aac':
        args = ['-c:a', 'aac', '-b:a', '256k']
    else:
        # fallback to copying streams (may fail); try libopus? but we stick to default
        args = ['-c:a', 'copy']

    ffmpeg_cmd = ['ffmpeg', '-v', 'quiet', '-y', '-i', src_path] + args + [tmp_path]
    try:
        subprocess.run(ffmpeg_cmd, check=True)
        # write tags to tmp file
        write_tags_generic(tmp_path, tags)
        return tmp_path
    except Exception as e:
        log(f"[!] Re-encode failed for {src_path}: {e}")
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except Exception:
            pass
        return None
# --------------------------------------------

def compute_fingerprint(path, max_seconds=FP_MAX_SECONDS, retries=FP_RETRIES):
    """
    Compute Chromaprint fingerprint using FFmpeg + Python bindings.
    Retries with decreasing length if crashes occur.
    On repeated failures, optionally re-encode the file (keeping same format) and try again.
    Returns SHA-1 hex string of the raw fingerprint, or None on failure.
    """
    attempt = 0
    while attempt < retries:
        seconds = max(FP_MIN_SECONDS, max_seconds - attempt * ((max_seconds - FP_MIN_SECONDS) // max(1, retries-1)))
        try:
            ffmpeg_cmd = [
                "ffmpeg", "-v", "quiet", "-i", path,
                "-f", "s16le",
                "-acodec", "pcm_s16le",
                "-ac", "2",
                "-ar", "44100",
                "-"
            ]

            p = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
            try:
                bytes_needed = int(seconds * 44100 * 2 * 2)
                pcm = p.stdout.read(bytes_needed)
            finally:
                try:
                    p.stdout.close()
                except Exception:
                    pass
                try:
                    p.kill()
                except Exception:
                    pass
                try:
                    p.wait(timeout=1)
                except Exception:
                    pass

            if not pcm:
                log(f"[!] FFmpeg produced no PCM data (attempt {attempt+1}/{retries}) for: {path}")
                attempt += 1
                continue

            fp = chromaprint.Fingerprinter()
            fp.feed(pcm)
            raw_fp = fp.finish()

            if not raw_fp:
                log(f"[!] Chromaprint returned empty fingerprint (attempt {attempt+1}/{retries}): {path}")
                attempt += 1
                continue

            return compute_sha1_fingerprint(raw_fp)

        except Exception as e:
            log(f"[!] Fingerprint attempt {attempt+1}/{retries} failed for {path}: {e}")
            attempt += 1

    log(f"[!] All fingerprint attempts failed for: {path}")

    # Try re-encoding if enabled and not already tried via temp file
    if REENCODE_ON_FAILURE:
        log(f"[i] Attempting re-encode to recover fingerprint for: {path}")
        tmp = reencode_file_same_format(path)
        if tmp:
            try:
                # try fingerprinting the reencoded file (single try with max_seconds)
                fp_tmp = compute_fingerprint_on_temp(tmp, max_seconds)
                if fp_tmp:
                    # replace original with reencoded file (safer: move reencoded over original)
                    try:
                        # preserve mtime
                        try:
                            orig_mtime = os.path.getmtime(path)
                        except Exception:
                            orig_mtime = None
                        shutil.move(tmp, path)
                        if orig_mtime:
                            os.utime(path, (orig_mtime, orig_mtime))
                        log(f"[✓] Re-encoded and replaced original: {path}")
                    except Exception as e:
                        log(f"[!] Failed to replace original with re-encoded file: {e}")
                        # if move failed leave tmp and continue with fingerprint from tmp
                    return fp_tmp
                else:
                    log(f"[!] Fingerprint still failed after re-encode for: {path}")
            finally:
                # cleanup temp if still exists
                if os.path.exists(tmp):
                    try:
                        os.remove(tmp)
                    except Exception:
                        pass
    return None

def compute_fingerprint_on_temp(tmp_path, max_seconds):
    """Helper to fingerprint a temporary file with single attempt (no re-encode recursion)."""
    try:
        ffmpeg_cmd = [
            "ffmpeg", "-v", "quiet", "-i", tmp_path,
            "-f", "s16le",
            "-acodec", "pcm_s16le",
            "-ac", "2",
            "-ar", "44100",
            "-"
        ]
        p = subprocess.Popen(ffmpeg_cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            bytes_needed = int(max_seconds * 44100 * 2 * 2)
            pcm = p.stdout.read(bytes_needed)
        finally:
            try:
                p.stdout.close()
            except Exception:
                pass
            try:
                p.kill()
            except Exception:
                pass
            try:
                p.wait(timeout=1)
            except Exception:
                pass

        if not pcm:
            return None

        fp = chromaprint.Fingerprinter()
        fp.feed(pcm)
        raw_fp = fp.finish()
        if not raw_fp:
            return None
        return compute_sha1_fingerprint(raw_fp)
    except Exception:
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
            # Strip leading track numbers like "01 - " or "1. "
            fname = re.sub(r'^\d+\s*[-\.]\s*', '', fname)
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

    # Compute fingerprint (use Python-native chromaprint)
    hash_fp = compute_fingerprint(path)

    # Move file first (regardless of fingerprint)
    if os.path.abspath(path) != os.path.abspath(dest_path):
        try:
            os.rename(path, dest_path)
            log(f"[✓] Moved {path} -> {dest_path}")
        except Exception as e:
            log(f"[!] Failed to move {path} -> {dest_path}: {e}")
            return
        path_to_store = dest_path
    else:
        path_to_store = path

    c = conn.cursor()
    status = "active"

    # Check for duplicates by fingerprint (if we have one)
    if hash_fp:
        c.execute("SELECT id, path, status, hash_fp FROM files WHERE hash_fp=?", (hash_fp,))
        row = c.fetchone()
        if row:
            orig_path = row[1]
            new_dest = get_unique_dest(dest_path)
            try:
                os.rename(path_to_store, new_dest)
                log(f"[DUPLICATE] {path_to_store} -> {new_dest}")
                path_to_store = new_dest
                status = "duplicate"
            except Exception as e:
                log(f"[!] Failed to rename duplicate {path_to_store} -> {new_dest}: {e}")
    else:
        # Fingerprint failed: fuzzy duplicate fallback
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
    try:
        mtime = int(os.path.getmtime(path_to_store))
    except Exception:
        mtime = now

    if existing:
        db_id = existing[0]
        db_hash_fp = existing[1]
        db_mtime = existing[2]
        db_status = existing[3]

        # Option A: replace old fingerprints (force recompute)
        # Detect "old" fingerprints that don't look like 40-char SHA1, or simply differ.
        need_update_fp = (db_hash_fp is None) or (len(db_hash_fp) != 40) or (db_hash_fp != hash_fp and hash_fp is not None)

        if need_update_fp:
            c.execute("""
                UPDATE files SET artist=?, album=?, title=?, track=?, hash_fp=?, file_mtime=?, status=? WHERE id=?
            """, (tags['artist'], tags['album'], tags['title'], tags['track'], hash_fp, mtime, status, db_id))
            log(f"[DB UPDATED - NEW FP] {path_to_store}")
        else:
            # Update metadata/mtime/status only if changed
            if db_mtime != mtime or db_status != status:
                c.execute("""
                    UPDATE files SET artist=?, album=?, title=?, track=?, file_mtime=?, status=? WHERE id=?
                """, (tags['artist'], tags['album'], tags['title'], tags['track'], mtime, status, db_id))
                log(f"[DB UPDATED] {path_to_store}")
    else:
        c.execute("""
            INSERT INTO files (path, artist, album, title, track, hash_fp, file_mtime, first_seen, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (path_to_store, tags['artist'], tags['album'], tags['title'], tags['track'], hash_fp, mtime, now, status))
        log(f"[DB INSERT] {path_to_store}")

    conn.commit()
# --------------------------------------------

def process_directory(source_dir, target_dir):
    conn = init_db(DB_FILE)
    # scan files
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
