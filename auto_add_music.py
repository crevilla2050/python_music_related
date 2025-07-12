#!/usr/bin/env python3

# auto_add_music.py

import os
import sys
import time
import shutil
import re
import json
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from organize_music import organize_file
from mutagen import File as MutagenFile
from mutagen.easyid3 import EasyID3
from musicbrainzngs import set_useragent, search_recordings

WATCH_FOLDER = os.path.expanduser("~/Music/AutoAdd")
FAILED_FOLDER = os.path.expanduser("~/FailedMusic")
SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']

# MusicBrainz setup
set_useragent("MusicOrganizer", "1.0", "carlos.revilla.m@gmail.com")

# Command-line args
if len(sys.argv) < 2:
    print("Usage: python auto_add_music.py <library_root_path> [alias_json_file]")
    sys.exit(1)

LIBRARY_ROOT = os.path.abspath(sys.argv[1])
alias_file = sys.argv[2] if len(sys.argv) >= 3 else None

# Load aliases if provided
artist_aliases = {}
album_aliases = {}
if alias_file:
    try:
        with open(alias_file, "r", encoding="utf-8") as f:
            aliases = json.load(f)
            artist_aliases = aliases.get("artist_aliases", {})
            album_aliases = aliases.get("album_aliases", {})
        print(f"[INFO] Loaded alias file: {alias_file}")
    except Exception as e:
        print(f"[!] Failed to load alias file '{alias_file}': {e}")
else:
    print("[INFO] No alias file provided. Proceeding without aliases.")

def has_reasonable_tags(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if not audio:
            return False
        artist = audio.get("artist", [None])[0]
        title = audio.get("title", [None])[0]
        return bool(artist and title)
    except Exception as e:
        print(f"[!] Error reading tags for {filepath}: {e}")
        return False

def enrich_tags(filepath, tags):
    try:
        audio = EasyID3(filepath)
    except Exception:
        audio = MutagenFile(filepath, easy=True)
        if audio is None:
            return
        audio.add_tags()
        audio = EasyID3(filepath)

    for key, value in tags.items():
        if key not in audio or not audio.get(key):
            audio[key] = value
    audio.save()

def musicbrainz_fallback(filepath):
    filename = os.path.splitext(os.path.basename(filepath))[0]
    if '-' not in filename:
        print(f"[!] Filename '{filename}' does not contain '-' to split artist/title")
        return None
    artist_guess, title_guess = map(str.strip, filename.split('-', 1))
    try:
        result = search_recordings(artist=artist_guess, recording=title_guess, limit=1)
        rec = result['recording-list'][0]
        artist = rec['artist-credit'][0]['artist']['name']
        title = rec['title']
        album = rec['release-list'][0]['title'] if 'release-list' in rec else 'Unknown'
        return {'artist': artist, 'title': title, 'album': album, 'tracknumber': '00'}
    except Exception as e:
        print(f"[!] MusicBrainz search failed for '{filename}': {e}")
        return None

def normalize_filename(name, track_number=None):
    name = re.sub(r'[\\/:*?"<>|]', '', name).strip()
    if track_number and track_number.isdigit():
        name = f"{track_number.zfill(2)} - {name}"
    return name

def is_duplicate(new_path):
    return os.path.exists(new_path)

def process_file(filepath):
    print(f"[DEBUG] Processing file: {filepath}")
    if not os.path.isfile(filepath):
        print(f"[!] Skipping non-file: {filepath}")
        return

    ext = os.path.splitext(filepath)[1].lower()
    if ext not in SUPPORTED_EXTS:
        print(f"[!] Unsupported file extension '{ext}', skipping: {filepath}")
        return

    tags = None
    if not has_reasonable_tags(filepath):
        print(f"[!] Incomplete tags, attempting MusicBrainz fallback for: {filepath}")
        tags = musicbrainz_fallback(filepath)
        if not tags:
            print(f"[✗] No tags found via MusicBrainz for: {filepath}")
            move_to_failed(filepath)
            return
        else:
            print(f"[✓] Tags found via MusicBrainz fallback: {tags}")
            enrich_tags(filepath, tags)
    else:
        print(f"[✓] File already has reasonable tags: {filepath}")

    new_path = organize_file(filepath, LIBRARY_ROOT, artist_aliases, album_aliases)
    if new_path:
        filename = os.path.basename(new_path)
        track_number = ""
        try:
            audio = MutagenFile(filepath, easy=True)
            track_number = audio.get("tracknumber", [""])[0]
        except Exception:
            pass

        normalized = normalize_filename(filename, track_number)
        new_path = os.path.join(os.path.dirname(new_path), normalized)

        if is_duplicate(new_path):
            print(f"[!] Duplicate file detected, skipping: {new_path}")
            move_to_failed(filepath)
            return

        try:
            shutil.move(filepath, new_path)
            print(f"[✓] Moved to: {new_path}")
        except Exception as e:
            print(f"[!] Failed to move file to {new_path}: {e}")
            move_to_failed(filepath)
    else:
        print(f"[!] Failed to organize: {filepath}")
        move_to_failed(filepath)

def move_to_failed(filepath):
    os.makedirs(FAILED_FOLDER, exist_ok=True)
    try:
        failed_path = os.path.join(FAILED_FOLDER, os.path.basename(filepath))
        shutil.move(filepath, failed_path)
        print(f"[→] Moved to Failed folder: {failed_path}")
    except Exception as e:
        print(f"[!] Failed to move to Failed folder: {e}")

class MusicHandler(FileSystemEventHandler):
    def on_created(self, event):
        print(f"[DEBUG] Event detected: {event.src_path}, is_directory={event.is_directory}")
        if event.is_directory:
            return
        time.sleep(1)
        process_file(event.src_path)

def process_existing_files():
    print(f"[INFO] Processing existing files in {WATCH_FOLDER} at startup...")
    for root, _, files in os.walk(WATCH_FOLDER):
        for file in files:
            filepath = os.path.join(root, file)
            process_file(filepath)

if __name__ == "__main__":
    os.makedirs(WATCH_FOLDER, exist_ok=True)

    process_existing_files()
    observer = Observer()
    observer.schedule(MusicHandler(), path=WATCH_FOLDER, recursive=True)
    observer.start()
    print(f"[INFO] Watching folder for new files: {WATCH_FOLDER}")

    try:
        while True:
            time.sleep(5)
    except KeyboardInterrupt:
        print("\n[INFO] Stopping observer...")
        observer.stop()
    observer.join()
