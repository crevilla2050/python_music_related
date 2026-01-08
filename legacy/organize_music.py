# This script organizes music files by moving them into artist/album directories,
# normalizing names, applying aliases, and logging duplicates.
# It uses the Mutagen library to read audio tags and supports various audio formats.
# It also computes SHA-256 hashes to detect duplicates and logs them in a JSON file.
# The script can be run from the command line with a source directory and an alias JSON file.
# If a target directory is not specified, it uses the source directory for organization.
# The aliases JSON file should contain "artist_aliases" and "album_aliases" keys
# with normalized names as keys and actual names as values.
# The script creates directories for artists and albums, normalizes filenames,
# and handles potential naming conflicts by appending "_dup" to duplicate filenames.
# It logs all operations, including errors, to a log file and outputs the results to the console.
# If duplicates are found, they are logged in a separate JSON file for further review.
# The script is designed to be robust against various file system issues and provides clear feedback on its operations.
# It can be easily extended to support additional audio formats or more complex organization schemes if needed.
# The script is intended for personal music libraries but can be adapted for larger collections or different organizational schemes.
# It is a useful tool for anyone looking to clean up and organize their music collection efficiently.
# The script can be run in a Python environment with the Mutagen library installed.
# It is recommended to run the script in a controlled environment first to ensure it behaves as expected.
# It is also advisable to back up your music collection before running the script to prevent accidental data loss.
# The script is designed to be run from the command line, making it easy to integrate
# into existing workflows or automation scripts.
# It can be scheduled to run periodically to keep the music library organized as new files are added.
# The script is open source and can be modified to suit individual needs or preferences.
# It is released under the MIT License, allowing for free use and modification.
# The script is intended to be user-friendly, with clear error messages and logging to help diagnose
# any issues that may arise during execution.
# It is designed to handle a wide range of audio file formats and naming conventions,
# making it versatile for different music collections.
# The script can be easily integrated into larger music management systems or used as a standalone tool.
# It is a practical solution for organizing music files based on metadata and file structure.   

# organize_music.py

# DEPRECATED
# Superseded by:
#   - consolidate_music.py
#   - disc_n_gen_aliases.py
#   - execute_actions.py
#   - FastAPI + UI review flow

import os
import sys
import re
import json
import shutil
import hashlib
import unicodedata
from mutagen import File as MutagenFile

SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']
MAX_FILENAME_LEN = 100
MAX_DIRNAME_LEN = 100
LOG_FILE = "organize_music.log"
DUPLICATES_LOG = "duplicate_files_log.json"

seen_hashes = {}
duplicates = []

# Define a function called log that takes a parameter called message
def log(message):
    # Open the LOG_FILE in append mode and set the encoding to utf-8
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        # Write the message to the LOG_FILE followed by a newline character
        f.write(message + "\n")
    # Print the message to the console
    print(message)

def compute_sha256(filepath, block_size=65536):
    # Create a SHA256 hasher object
    hasher = hashlib.sha256()
    try:
        # Open the file in binary read mode
        with open(filepath, 'rb') as f:
            # Read the file in chunks of block_size
            while chunk := f.read(block_size):
                # Update the hasher with the chunk
                hasher.update(chunk)
        # Return the hexdigest of the hasher
        return hasher.hexdigest()
    except Exception as e:
        # Log an error message if an exception is raised
        log(f"[!] Error hashing file {filepath}: {e}")
        # Return None if an exception is raised
        return None

def normalize_name(name, max_len=MAX_DIRNAME_LEN):
    # Normalize the name using NFKD normalization
    name = unicodedata.normalize("NFKD", name)
    # Remove any combining characters
    name = ''.join(c for c in name if not unicodedata.combining(c))
    # Replace any characters that are not alphanumeric or a space with an underscore
    name = re.sub(r'[<>:"/\\|?*]', '_', name)
    # Replace any sequences of spaces and periods with an underscore
    name = re.sub(r'\s*\.+\s*', '_', name)
    # Remove any leading or trailing spaces or periods
    name = name.strip(' .')
    # Remove any characters that are not alphanumeric or a space, underscore, dash, or parentheses
    name = ''.join(c for c in name if c.isalnum() or c in " _-().[]").strip()
    # Return the name truncated to the maximum length
    return name[:max_len]

# Define a function called normalize_filename that takes in two parameters, name and track
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

# Define a function called load_aliases that takes a json_file as an argument
def load_aliases(json_file):
    # Try to open the json_file and load the data
    try:
        with open(json_file, encoding="utf-8") as f:
            data = json.load(f)
            # Return the artist_aliases and album_aliases from the data
            return data.get("artist_aliases", {}), data.get("album_aliases", {})
    # If an exception is raised, log the error and return empty dictionaries
    except Exception as e:
        log(f"[!] Failed to load alias file: {e}")
        return {}, {}

# Define a function called apply_alias that takes two parameters, name and aliases
def apply_alias(name, aliases):
    # Normalize the name using the normalize_name function
    norm = normalize_name(name)
    # Return the alias if it exists, otherwise return the original name
    return aliases.get(norm, name)

def extract_tags(filepath):
    # Try to extract tags from the given file
    try:
        # Create an instance of the MutagenFile class with the given file path
        audio = MutagenFile(filepath, easy=True)
        # If the audio object is not created, return None
        if not audio:
            return None
        # Return a dictionary containing the extracted tags
        return {
            "artist": audio.get("artist", ["Unknown Artist"])[0].strip(),
            "album": audio.get("album", ["Unknown Album"])[0].strip(),
            "title": audio.get("title", ["Unknown Title"])[0].strip(),
            "track": audio.get("tracknumber", [""])[0].split("/")[0].strip()
        }
    # If an exception is raised, log the error and return None
    except Exception as e:
        log(f"[!] Error extracting tags from '{filepath}': {e}")
        return None

# Define a function to get a unique destination path
def get_unique_dest(dest_path):
    # Split the destination path into base and extension
    base, ext = os.path.splitext(dest_path)
    # Initialize a count variable
    count = 1
    # Loop until a unique path is found
    while os.path.exists(dest_path):
        # Create a new path with a duplicate suffix
        dest_path = f"{base}_dup{count}{ext}"
        # Increment the count
        count += 1
    # Return the unique path
    return dest_path

def organize_file(filepath, target_root, artist_aliases, album_aliases):
    # Try to extract tags from the file
    try:
        tags = extract_tags(filepath)
        # If no tags are found, return
        if not tags:
            return

        # Apply aliases to the artist and album names
        artist = apply_alias(tags["artist"], artist_aliases)
        album = apply_alias(tags["album"], album_aliases)
        # Get the title and track number from the tags
        title = tags["title"]
        track = tags["track"]

        # Create the base directory for the file
        base_dir = os.path.join(target_root, normalize_name(artist), normalize_name(album))
        # Create the directory if it doesn't exist
        os.makedirs(base_dir, exist_ok=True)

        # Create the filename for the file
        filename = normalize_filename(f"{artist} - {title}{os.path.splitext(filepath)[1]}", track)
        # Create the destination path for the file
        dest_path = os.path.join(base_dir, filename)

        # Compute the SHA256 hash of the file
        file_hash = compute_sha256(filepath)
        # If no hash is found, return
        if not file_hash:
            return

        # Check if the file hash is already in the seen_hashes dictionary
        if file_hash in seen_hashes:
            # If it is, log a duplicate detected message
            log(f"[!] Duplicate detected: {filepath} (same as {seen_hashes[file_hash]})")
            # Get a unique destination path for the file
            dest_path = get_unique_dest(dest_path)
            # Add the duplicate information to the duplicates list
            duplicates.append({
                "duplicate": filepath,
                "original": seen_hashes[file_hash],
                "destination": dest_path
            })
        else:
            # If the file hash is not in the seen_hashes dictionary, add it
            seen_hashes[file_hash] = filepath

        # If the file is not already in the destination path, move it
        if os.path.abspath(filepath) != os.path.abspath(dest_path):
            shutil.move(filepath, dest_path)
            log(f"[✓] Moved: {filepath} -> {dest_path}")
        else:
            # If the file is already in the destination path, log a message
            log(f"[=] Already in place: {filepath}")
    # If an OSError occurs, log an error message
    except OSError as e:
        log(f"[!] Failed to create directory or move file '{filepath}': {e}")
    # If any other exception occurs, log an error message
    except Exception as e:
        log(f"[!] Unexpected error processing '{filepath}': {e}")

def process_directory(source_dir, target_dir, alias_file):
    # Check if the source directory exists
    if not os.path.isdir(source_dir):
        log(f"[✗] Source directory does not exist: {source_dir}")
        return

    # Load the aliases from the alias file
    artist_aliases, album_aliases = load_aliases(alias_file)
    log(f"[INFO] Loaded aliases from {alias_file}")

    # Walk through the source directory
    for dirpath, _, filenames in os.walk(source_dir):
        for fname in filenames:
            # Get the file extension
            ext = os.path.splitext(fname)[1].lower()
            # Check if the file extension is supported
            if ext not in SUPPORTED_EXTS:
                continue
            # Get the full path of the file
            full_path = os.path.join(dirpath, fname)
            # Organize the file
            organize_file(full_path, target_dir, artist_aliases, album_aliases)

    # Check if there are any duplicates
    if duplicates:
        # Log the duplicates
        with open(DUPLICATES_LOG, "w", encoding="utf-8") as f:
            json.dump(duplicates, f, indent=2, ensure_ascii=False)
        log(f"[✓] Duplicates logged in {DUPLICATES_LOG}")
    else:
        log("[✓] No duplicates found.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python organize_music.py <source_dir> <aliases_json> [<target_dir>]")
        sys.exit(1)

    source_dir = sys.argv[1]
    alias_file = sys.argv[2]
    target_dir = sys.argv[3] if len(sys.argv) > 3 else source_dir

    process_directory(source_dir, target_dir, alias_file)
