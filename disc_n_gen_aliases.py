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
    # Initialize an empty list to store the files
    files = []
    # Initialize two dictionaries to store the variants of artists and albums
    artist_variants = {}
    album_variants = {}

    # Iterate through the root path and its subdirectories
    for dirpath, _, filenames in os.walk(root_path):
        # Iterate through the filenames in the current directory
        for fname in filenames:
            # Get the file extension
            ext = os.path.splitext(fname)[1].lower()
            # If the file extension is not supported, skip it
            if ext not in SUPPORTED_EXTS:
                continue

            # Get the full path of the file
            full_path = os.path.join(dirpath, fname)
            # Log the file being processed
            log(f"[*] Processing file: {full_path}")
            # Get the tags of the file
            tags = get_tags(full_path)
            # Calculate the hash of the file
            hash_val = calculate_hash(full_path)
            # Normalize the file name
            norm_name = normalize_string(fname)

            # Append the file information to the list
            files.append({
                "path": full_path,
                "name": fname,
                "norm_name": norm_name,
                "tags": tags,
                "hash": hash_val
            })

            # If the file has an artist tag, add it to the artist variants dictionary
            if tags.get("artist"):
                artist_raw = tags["artist"]
                norm_artist = normalize_string(artist_raw)
                artist_variants.setdefault(norm_artist, set()).add(artist_raw)

            # If the file has an album tag, add it to the album variants dictionary
            if tags.get("album"):
                album_raw = tags["album"]
                norm_album = normalize_string(album_raw)
                album_variants.setdefault(norm_album, set()).add(album_raw)

    # Return the list of files, artist variants, and album variants
    return files, artist_variants, album_variants

# Define a function to scan the folder structure of a given root path
def scan_folder_structure(root_path):
    # Create two dictionaries to store the variants of artists and albums
    artist_variants = {}
    album_variants = {}

    # Iterate through the root path and its subdirectories
    for dirpath, dirnames, _ in os.walk(root_path):
        # Get the relative path of the current directory
        rel_path = os.path.relpath(dirpath, root_path)
        # Skip the root directory
        if rel_path == ".":
            continue
        # Split the relative path into parts
        parts = rel_path.split(os.sep)

        # If the relative path has only one part and it is not "collections"
        if len(parts) == 1 and parts[0].lower() != "collections":
            # Get the raw artist name
            artist_raw = parts[0]
            # Normalize the artist name
            norm_artist = normalize_string(artist_raw)
            # Add the raw artist name to the set of variants for the normalized artist name
            artist_variants.setdefault(norm_artist, set()).add(artist_raw)

        # If the relative path has two parts
        if len(parts) == 2:
            # Get the raw album name
            album_raw = parts[1]
            # Normalize the album name
            norm_album = normalize_string(album_raw)
            # Add the raw album name to the set of variants for the normalized album name
            album_variants.setdefault(norm_album, set()).add(album_raw)

            # If the first part of the relative path is not "collections"
            if parts[0].lower() != "collections":
                # Get the raw artist name
                artist_raw = parts[0]
                # Normalize the artist name
                norm_artist = normalize_string(artist_raw)
                # Add the raw artist name to the set of variants for the normalized artist name
                artist_variants.setdefault(norm_artist, set()).add(artist_raw)

    # Return the dictionaries of artist and album variants
    return artist_variants, album_variants

def build_aliases(variants_dict, threshold=SIMILARITY_THRESHOLD):
    # Create a set to store the keys that have been seen
    seen = set()
    # Create a dictionary to store the aliases
    aliases = {}
    # Create a list of the keys in the variants_dict
    keys = list(variants_dict.keys())

    # Iterate through the keys
    for i, key in enumerate(keys):
        # If the key has already been seen, skip it
        if key in seen:
            continue

        # Create a list to store the group of keys that are similar
        group = [key]
        # Get the closest matches to the key
        matches = difflib.get_close_matches(key, keys, n=10, cutoff=threshold)
        # Iterate through the matches
        for match in matches:
            # If the match is not the key and has not been seen, add it to the group
            if match != key and match not in seen:
                group.append(match)

        # Create a set to store the combined variants
        combined = set()
        # Iterate through the group
        for g in group:
            # Add the variants to the combined set
            combined.update(variants_dict.get(g, []))
            # Add the key to the seen set
            seen.add(g)

        # Sort the combined set by length and get the last element
        canonical = sorted(combined, key=len)[-1]
        # Iterate through the combined set
        for alias in combined:
            # If the alias is not the canonical, add it to the aliases dictionary
            if alias != canonical:
                aliases[alias] = canonical

        # Log the group and canonical
        log(f"[~] Group: {group} → Canonical: {canonical}")

    # Return the aliases dictionary
    return aliases

def merge_variants(dict1, dict2):
    # Create a copy of the first dictionary
    merged = dict1.copy()
    # Iterate through the second dictionary
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
