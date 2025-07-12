#!/usr/bin/env python3

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

VERBOSE = False  # Set dynamically from CLI

def log(msg):
    if VERBOSE:
        print(msg)

def normalize_string(s):
    s = s.lower()
    s = unicodedata.normalize('NFKD', s)
    s = ''.join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r'^\d+\s*[-._)]*\s*', '', s)
    s = re.sub(r'\(.*?\)|\[.*?\]', '', s)
    s = re.sub(r'feat\.?.*|ft\.?.*|remastered|live|version', '', s)
    s = re.sub(r'[^a-z0-9 ]+', '', s)
    s = re.sub(r'\s+', ' ', s)
    return s.strip()

def get_tags(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if not audio:
            return {}
        tags = {
            "artist": audio.get("artist", [""])[0].strip(),
            "title": audio.get("title", [""])[0].strip(),
            "album": audio.get("album", [""])[0].strip(),
        }
        log(f"  [✓] Tags for {os.path.basename(filepath)}: {tags}")
        return tags
    except Exception as e:
        log(f"  [!] Failed to read tags for {filepath}: {e}")
        return {}

def calculate_hash(filepath, algo='sha256', block_size=65536):
    try:
        h = hashlib.new(algo)
        with open(filepath, 'rb') as f:
            for chunk in iter(lambda: f.read(block_size), b''):
                h.update(chunk)
        hash_val = h.hexdigest()
        log(f"  [✓] Hash for {os.path.basename(filepath)}: {hash_val[:10]}...")
        return hash_val
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
            log(f"[*] Processing file: {full_path}")
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
                artist_raw = tags["artist"]
                norm_artist = normalize_string(artist_raw)
                artist_variants.setdefault(norm_artist, set()).add(artist_raw)

            if tags.get("album"):
                album_raw = tags["album"]
                norm_album = normalize_string(album_raw)
                album_variants.setdefault(norm_album, set()).add(album_raw)

    return files, artist_variants, album_variants

def scan_folder_structure(root_path):
    artist_variants = {}
    album_variants = {}

    for dirpath, dirnames, _ in os.walk(root_path):
        rel_path = os.path.relpath(dirpath, root_path)
        if rel_path == ".":
            continue
        parts = rel_path.split(os.sep)

        if len(parts) == 1 and parts[0].lower() != "collections":
            artist_raw = parts[0]
            norm_artist = normalize_string(artist_raw)
            artist_variants.setdefault(norm_artist, set()).add(artist_raw)

        if len(parts) == 2:
            album_raw = parts[1]
            norm_album = normalize_string(album_raw)
            album_variants.setdefault(norm_album, set()).add(album_raw)

            if parts[0].lower() != "collections":
                artist_raw = parts[0]
                norm_artist = normalize_string(artist_raw)
                artist_variants.setdefault(norm_artist, set()).add(artist_raw)

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

        log(f"[~] Group: {group} → Canonical: {canonical}")

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
    # Check if the files have the same hash
    if file1['hash'] and file2['hash'] and file1['hash'] == file2['hash']:
        return True

    # Get the tags from the files
    tags1 = file1["tags"]
    tags2 = file2["tags"]
    # Check if the files have tags
    if tags1 and tags2:
        matches = 0
        total = 0
        # Loop through the tags
        for key in ["artist", "title", "album"]:
            # Get the value of the tag
            val1 = normalize_string(tags1.get(key, ""))
            val2 = normalize_string(tags2.get(key, ""))
            # Check if the tag exists in both files
            if val1 and val2:
                total += 1
                # Check if the tags are the same or similar
                if val1 == val2 or difflib.SequenceMatcher(None, val1, val2).ratio() > SIMILARITY_THRESHOLD:
                    matches += 1
        # Check if the percentage of matches is greater than 66%
        if total > 0 and matches / total >= 0.66:
            return True

    # Check if the file names are similar
    similarity = difflib.SequenceMatcher(None, file1["norm_name"], file2["norm_name"]).ratio()
    return similarity > SIMILARITY_THRESHOLD

def find_duplicates(files):
    # Create an empty set to store the indices of files that have been seen
    seen = set()
    # Create an empty list to store the groups of duplicate files
    groups = []

    # Loop through the list of files
    for i in range(len(files)):
        # If the current file has already been seen, skip it
        if i in seen:
            continue
        # Create a new group with the current file
        group = [files[i]]
        # Loop through the remaining files
        for j in range(i + 1, len(files)):
            # If the current file has already been seen, skip it
            if j in seen:
                continue
            # If the current file is similar to the previous file, add it to the group
            if are_files_similar(files[i], files[j]):
                log(f"  [=] Duplicate: {files[i]['path']} <==> {files[j]['path']}")
                group.append(files[j])
                # Add the index of the current file to the set of seen files
                seen.add(j)
        # If the group contains more than one file, add it to the list of groups
        if len(group) > 1:
            groups.append(group)
            # Add the index of the current file to the set of seen files
            seen.add(i)
    # Return the list of groups
    return groups

def main():
    # Set the global variable VERBOSE to False
    global VERBOSE

    # Check if the number of arguments is less than 3
    if len(sys.argv) < 3:
        # Print the usage of the program
        print("Usage: python music_manager.py <music_dir> --mode [aliases|duplicates|all] [--verbose]")
        # Exit the program with a status code of 1
        sys.exit(1)

    # Get the arguments from the command line
    args = sys.argv[1:]
    # Get the root directory from the arguments
    root_dir = args[0]
    # Set the mode to "all"
    mode = "all"
    # Check if the "--mode" argument is in the arguments
    if "--mode" in args:
        # Get the index of the "--mode" argument
        mode_index = args.index("--mode")
        # Check if the index of the "--mode" argument is less than the length of the arguments
        if mode_index + 1 < len(args):
            # Set the mode to the argument after the "--mode" argument
            mode = args[mode_index + 1].lower()

    # Set the VERBOSE variable to True if the "--verbose" argument is in the arguments
    VERBOSE = "--verbose" in args

    # Print the root directory
    print(f"[*] Scanning directory: {root_dir}")
    # Print the mode
    print(f"[*] Mode: {mode}")
    # Check if the VERBOSE variable is True
    if VERBOSE:
        # Print that the verbose mode is ON
        print("[*] Verbose mode ON")

    # Scan the music files in the root directory
    files, tag_artists, tag_albums = scan_music_files(root_dir)
    # Scan the folder structure in the root directory
    folder_artists, folder_albums = scan_folder_structure(root_dir)
    # Merge the variants of the artists and albums
    artist_variants = merge_variants(tag_artists, folder_artists)
    album_variants = merge_variants(tag_albums, folder_albums)

    # Check if the mode is "aliases" or "all"
    if mode in ("aliases", "all"):
        # Print that the artist/album aliases are being generated
        print("[*] Generating artist/album aliases...")
        # Build the aliases for the artists
        artist_aliases = build_aliases(artist_variants)
        # Build the aliases for the albums
        album_aliases = build_aliases(album_variants)
        # Write the aliases to a JSON file
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
        # Find the duplicate groups
        dup_groups =_
