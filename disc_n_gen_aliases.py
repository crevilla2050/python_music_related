#!/usr/bin/env python3

# disc_n_gen_aliases.py
# This script scans a music directory for audio files, extracts metadata,
# generates aliases for artists and albums, and finds duplicate files based on metadata and file content.
# It outputs the results to JSON files for further processing or review.
# Ensure you have the required libraries installed:
# pip install mutagen
# Usage: python disc_n_gen_aliases.py <music_dir> --mode [aliases|duplicates|all] [--verbose]
# Example: python disc_n_gen_aliases.py /path/to/music --mode all --verbose
# The script supports various audio formats and normalizes names for better matching.
# It can be used to clean up music libraries by identifying duplicates and standardizing artist/album names.
# The output files are artist_album_aliases.json and duplicates.json.
# Adjust the SIMILARITY_THRESHOLD and SUPPORTED_EXTS as needed for your use case.
# The script is designed to be run from the command line and can handle large music collections efficiently.
# It uses hashing to compare files and difflib for string similarity checks.
# Make sure to run it in an environment where you have read access to the music directory.
# The script is compatible with Python 3 and requires the Mutagen library for audio file handling.
# It is a standalone script and does not require any additional configuration files.
# The output JSON files can be easily parsed or imported into other applications for further analysis.
# The script is designed to be efficient and should handle large directories without significant performance issues.
# It is recommended to run the script in a virtual environment to avoid conflicts with other Python packages.
# The script can be modified to include additional features such as logging to a file or more advanced duplicate detection algorithms.
# Feel free to contribute improvements or report issues on the project's repository.
# The script is released under the MIT License, allowing for free use and modification.
# For any questions or support, please refer to the documentation or contact the author.
# Enjoy organizing your music collection with this tool!    

import os
import sys
import json
import re
import hashlib
import unicodedata
import difflib
from mutagen import File as MutagenFile

SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']
SIMILARITY_THRESHOLD = 0.87
VERBOSE = False

def log(msg):
    if VERBOSE:
        print(msg)

def normalize_string(s):
    # Convert the string to lowercase
    s = s.lower()
    # Normalize the string to decompose any combined characters
    s = unicodedata.normalize('NFKD', s)
    # Remove any combining characters
    s = ''.join(c for c in s if not unicodedata.combining(c))
    # Remove any numbers, spaces, and punctuation at the beginning of the string
    s = re.sub(r'^\d+\s*[-._)]*\s*', '', s)
    # Remove any text within parentheses or brackets
    s = re.sub(r'\(.*?\)|\[.*?\]', '', s)
    # Remove any characters that are not letters, numbers, or spaces
    s = re.sub(r'[^a-z0-9 ]+', '', s)
    # Replace any multiple spaces with a single space
    s = re.sub(r'\s+', ' ', s)
    # Remove any leading or trailing spaces
    return s.strip()

def get_tags(filepath):
    # Try to read the tags from the given file
    try:
        audio = MutagenFile(filepath, easy=True)
        # If the file is not valid, return an empty dictionary
        if not audio:
            return {}
        # Return a dictionary containing the artist, title, and album
        return {
            "artist": audio.get("artist", [""])[0].strip(),
            "title": audio.get("title", [""])[0].strip(),
            "album": audio.get("album", [""])[0].strip(),
        }
    # If an exception is raised, log the error and return an empty dictionary
    except Exception as e:
        log(f"  [!] Failed to read tags for {filepath}: {e}")
        return {}

def calculate_hash(filepath, algo='sha256', block_size=65536):
    # Try to calculate the hash of the file
    try:
        # Create a new hash object using the specified algorithm
        h = hashlib.new(algo)
        # Open the file in binary mode
        with open(filepath, 'rb') as f:
            # Iterate over the file in chunks of the specified block size
            for chunk in iter(lambda: f.read(block_size), b''):
                # Update the hash object with the chunk
                h.update(chunk)
        # Return the hexadecimal representation of the hash
        return h.hexdigest()
    # If an exception is raised, log the error and return None
    except Exception as e:
        log(f"  [!] Hashing failed for {filepath}: {e}")
        return None

def scan_music_files(root_path):
    files = []
    artist_variants = {}
    album_variants = {}

    for dirpath, _, filenames in os.walk(root_path):
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in SUPPORTED_EXTS:
                continue

            full_path = os.path.join(dirpath, fname)
            tags = get_tags(full_path)
            hash_val = calculate_hash(full_path)
            norm_name = normalize_string(fname)

            files.append({
                "path": full_path,
                "name": fname,
                "norm_name": norm_name,
                "tags": tags,
                "hash": hash_val
            })

            if tags.get("artist"):
                norm_artist = normalize_string(tags["artist"])
                artist_variants.setdefault(norm_artist, set()).add(tags["artist"])
            if tags.get("album"):
                norm_album = normalize_string(tags["album"])
                album_variants.setdefault(norm_album, set()).add(tags["album"])

    return files, artist_variants, album_variants

def scan_folder_structure(root_path):
    artist_variants = {}
    album_variants = {}

    for dirpath, _, _ in os.walk(root_path):
        rel_path = os.path.relpath(dirpath, root_path)
        if rel_path == ".":
            continue
        parts = rel_path.split(os.sep)
        if len(parts) == 1 and parts[0].lower() != "collections":
            norm_artist = normalize_string(parts[0])
            artist_variants.setdefault(norm_artist, set()).add(parts[0])
        if len(parts) == 2:
            norm_album = normalize_string(parts[1])
            album_variants.setdefault(norm_album, set()).add(parts[1])
            if parts[0].lower() != "collections":
                norm_artist = normalize_string(parts[0])
                artist_variants.setdefault(norm_artist, set()).add(parts[0])

    return artist_variants, album_variants

def build_aliases(variants_dict, threshold=SIMILARITY_THRESHOLD):
    seen = set()
    aliases = {}
    keys = list(variants_dict.keys())

    for i, key in enumerate(keys):
        if key in seen:
            continue
        group = [key]
        matches = difflib.get_close_matches(key, keys, n=10, cutoff=threshold)
        for match in matches:
            if match != key and match not in seen:
                group.append(match)
        combined = set()
        for g in group:
            combined.update(variants_dict.get(g, []))
            seen.add(g)
        canonical = sorted(combined, key=len)[-1]
        for alias in combined:
            if alias != canonical:
                aliases[alias] = canonical
    return aliases

def merge_variants(dict1, dict2):
    merged = dict1.copy()
    for norm, variants in dict2.items():
        if norm in merged:
            merged[norm].update(variants)
        else:
            merged[norm] = set(variants)
    return merged

def are_files_similar(file1, file2):
    if file1['hash'] and file2['hash'] and file1['hash'] == file2['hash']:
        return True
    tags1, tags2 = file1["tags"], file2["tags"]
    if tags1 and tags2:
        matches = 0
        total = 0
        for key in ["artist", "title", "album"]:
            val1 = normalize_string(tags1.get(key, ""))
            val2 = normalize_string(tags2.get(key, ""))
            if val1 and val2:
                total += 1
                if val1 == val2 or difflib.SequenceMatcher(None, val1, val2).ratio() > SIMILARITY_THRESHOLD:
                    matches += 1
        if total > 0 and matches / total >= 0.66:
            return True
    similarity = difflib.SequenceMatcher(None, file1["norm_name"], file2["norm_name"]).ratio()
    return similarity > SIMILARITY_THRESHOLD

def find_duplicates(files):
    # Create an empty set to store the indices of files that have been seen
    seen = set()
    # Create an empty list to store the groups of duplicate files
    groups = []
    # Iterate through the list of files
    for i in range(len(files)):
        # If the current file has already been seen, skip it
        if i in seen:
            continue
        # Create a new group with the current file
        group = [files[i]]
        # Iterate through the remaining files
        for j in range(i + 1, len(files)):
            # If the current file has already been seen, skip it
            if j in seen:
                continue
            # If the current file is similar to the previous file, add it to the group and mark it as seen
            if are_files_similar(files[i], files[j]):
                group.append(files[j])
                seen.add(j)
        # If the group contains more than one file, add it to the list of groups and mark the first file as seen
        if len(group) > 1:
            groups.append(group)
            seen.add(i)
    # Return the list of groups
    return groups

def main():
    # Set the global variable VERBOSE to False by default
    global VERBOSE

    # Check if the user has provided the correct number of arguments
    if len(sys.argv) < 3:
        # Print the correct usage of the script
        print("Usage: python disc_n_gen_aliases.py <music_dir> --mode [aliases|duplicates|all] [--verbose]")
        # Exit the script with a status code of 1
        sys.exit(1)

    # Get the arguments passed to the script
    args = sys.argv[1:]
    # Get the root directory from the arguments
    root_dir = args[0]
    # Set the mode to "all" by default
    mode = "all"
    # Check if the user has provided the --mode argument
    if "--mode" in args:
        # Get the index of the --mode argument
        mode_index = args.index("--mode")
        # Check if the --mode argument is followed by a valid mode
        if mode_index + 1 < len(args):
            # Set the mode to the provided mode
            mode = args[mode_index + 1].lower()
    # Set the VERBOSE variable to True if the user has provided the --verbose argument
    VERBOSE = "--verbose" in args

    # Print the root directory and mode
    print(f"[*] Scanning directory: {root_dir}")
    print(f"[*] Mode: {mode}")
    # Check if the VERBOSE variable is True
    if VERBOSE:
        # Print that the VERBOSE mode is ON
        print("[*] Verbose mode ON")

    # Scan the music files in the root directory
    files, tag_artists, tag_albums = scan_music_files(root_dir)
    # Scan the folder structure in the root directory
    folder_artists, folder_albums = scan_folder_structure(root_dir)
    # Merge the variants from the tag and folder structure
    artist_variants = merge_variants(tag_artists, folder_artists)
    album_variants = merge_variants(tag_albums, folder_albums)

    # Check if the mode is "aliases" or "all"
    if mode in ("aliases", "all"):
        # Print that the artist/album aliases are being generated
        print("[*] Generating artist/album aliases...")
        # Build the artist/album aliases
        artist_aliases = build_aliases(artist_variants)
        album_aliases = build_aliases(album_variants)
        # Write the artist/album aliases to a JSON file
        with open("artist_album_aliases.json", "w", encoding="utf-8") as f:
            json.dump({
                "artist_aliases": artist_aliases,
                "album_aliases": album_aliases
            }, f, indent=2, ensure_ascii=False)
        # Print that the aliases have been written to the JSON file
        print(f"[✓] Aliases written to artist_album_aliases.json")

    # Check if the mode is "duplicates" or "all"
    if mode in ("duplicates", "all"):
        # Print that the duplicate files are being looked for
        print("[*] Looking for duplicate files...")
        # Find the duplicate files
        dup_groups = find_duplicates(files)
        # Write the duplicate files to a JSON file
        with open("duplicates.json", "w", encoding="utf-8") as f:
            json.dump(dup_groups, f, indent=2, ensure_ascii=False)
        # Print that the duplicate files have been written to the JSON file
        print(f"[✓] {len(dup_groups)} duplicate groups written to duplicates.json")

if __name__ == "__main__":
    main()
