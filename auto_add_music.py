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
set_useragent("MusicOrganizer", "1.0", "yourmail@example.com")

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
    # Try to read the tags of the audio file
    try:
        audio = MutagenFile(filepath, easy=True)
        # If the audio file is not found, return False
        if not audio:
            return False
        # Get the artist and title of the audio file
        artist = audio.get("artist", [None])[0]
        title = audio.get("title", [None])[0]
        # Return True if both artist and title are not None, otherwise return False
        return bool(artist and title)
    # If an exception is raised, print the error message and return False
    except Exception as e:
        print(f"[!] Error reading tags for {filepath}: {e}")
        return False

def enrich_tags(filepath, tags):
    # Try to load the audio file using EasyID3
    try:
        audio = EasyID3(filepath)
    # If an exception is raised, try to load the audio file using MutagenFile
    except Exception:
        audio = MutagenFile(filepath, easy=True)
        # If MutagenFile returns None, return
        if audio is None:
            return
        # Add tags to the audio file
        audio.add_tags()
        # Reload the audio file using EasyID3
        audio = EasyID3(filepath)

    # Loop through the tags dictionary
    for key, value in tags.items():
        # If the key is not in the audio file or the value is None, add the key-value pair to the audio file
        if key not in audio or not audio.get(key):
            audio[key] = value
    # Save the audio file
    audio.save()

def musicbrainz_fallback(filepath):
    # Get the filename from the filepath
    filename = os.path.splitext(os.path.basename(filepath))[0]
    # Check if the filename contains a '-' to split artist and title
    if '-' not in filename:
        print(f"[!] Filename '{filename}' does not contain '-' to split artist/title")
        return None
    # Split the filename into artist and title
    artist_guess, title_guess = map(str.strip, filename.split('-', 1))
    try:
        # Search for the recording on MusicBrainz
        result = search_recordings(artist=artist_guess, recording=title_guess, limit=1)
        # Get the first recording from the result
        rec = result['recording-list'][0]
        # Get the artist and title from the recording
        artist = rec['artist-credit'][0]['artist']['name']
        title = rec['title']
        # Get the album from the recording
        album = rec['release-list'][0]['title'] if 'release-list' in rec else 'Unknown'
        # Return the artist, title, album and tracknumber
        return {'artist': artist, 'title': title, 'album': album, 'tracknumber': '00'}
    except Exception as e:
        # Print an error message if the search fails
        print(f"[!] MusicBrainz search failed for '{filename}': {e}")
        return None

def normalize_filename(name, track_number=None):
    # Split the filename into name and extension
    name_only, ext = os.path.splitext(name)
    # Remove any illegal characters from the name
    name_only = re.sub(r'[\\/:*?"<>|]', '', name_only).strip()

    # If a track number is provided and it is a digit, add it to the name
    if track_number and track_number.isdigit():
        name_only = f"{track_number.zfill(2)}. {name_only}"

    # Return the normalized filename
    return f"{name_only}{ext}"


#Define a function called is_duplicate that takes in a parameter new_path
def is_duplicate(new_path):
    #Check if the new_path exists in the current directory
    return os.path.exists(new_path)

def process_file(filepath):
    # Print a debug message indicating the file being processed
    print(f"[DEBUG] Processing file: {filepath}")
    # Check if the file exists
    if not os.path.isfile(filepath):
        # If the file does not exist, print a message and return
        print(f"[!] Skipping non-file: {filepath}")
        return

    # Get the file extension
    ext = os.path.splitext(filepath)[1].lower()
    # Check if the file extension is supported
    if ext not in SUPPORTED_EXTS:
        # If the file extension is not supported, print a message and return
        print(f"[!] Unsupported file extension '{ext}', skipping: {filepath}")
        return

    # Initialize the tags variable
    tags = None
    # Check if the file has reasonable tags
    if not has_reasonable_tags(filepath):
        # If the file does not have reasonable tags, print a message and attempt a MusicBrainz fallback
        print(f"[!] Incomplete tags, attempting MusicBrainz fallback for: {filepath}")
        # Attempt a MusicBrainz fallback
        tags = musicbrainz_fallback(filepath)
        # Check if the MusicBrainz fallback found any tags
        if not tags:
            # If no tags were found, print a message and move the file to the failed directory
            print(f"[✗] No tags found via MusicBrainz for: {filepath}")
            move_to_failed(filepath)
            return
        else:
            # If tags were found, print a message and enrich the tags
            print(f"[✓] Tags found via MusicBrainz fallback: {tags}")
            enrich_tags(filepath, tags)
    else:
        # If the file already has reasonable tags, print a message
        print(f"[✓] File already has reasonable tags: {filepath}")

    # Organize the file
    new_path = organize_file(filepath, LIBRARY_ROOT, artist_aliases, album_aliases)
    # Check if the file was organized successfully
    if new_path:
        # If the file was organized successfully, get the filename and track number
        filename = os.path.basename(new_path)
        track_number = ""
        try:
            # Try to get the track number from the audio file
            audio = MutagenFile(filepath, easy=True)
            track_number = audio.get("tracknumber", [""])[0]
        except Exception:
            # If an exception is raised, pass
            pass

        # Normalize the filename
        normalized = normalize_filename(filename, track_number)
        # Create the new path
        new_path = os.path.join(os.path.dirname(new_path), normalized)

        # Check if the new path is a duplicate
        if is_duplicate(new_path):
            # If the new path is a duplicate, print a message and move the file to the failed directory
            print(f"[!] Duplicate file detected, skipping: {new_path}")
            move_to_failed(filepath)
            return

        try:
            # Try to move the file to the new path
            shutil.move(filepath, new_path)
            # If the file was moved successfully, print a message
            print(f"[✓] Moved to: {new_path}")
        except Exception as e:
            # If an exception is raised, print a message and move the file to the failed directory
            print(f"[!] Failed to move file to {new_path}: {e}")
            move_to_failed(filepath)
    else:
        # If the file was not organized successfully, print a message and move the file to the failed directory
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
