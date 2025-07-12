# find_duplicates.py
import os
import sys
import hashlib
import shutil
from difflib import SequenceMatcher
from mutagen import File as MutagenFile

SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg']

def compute_hash(filepath, block_size=65536):
    hasher = hashlib.sha1()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(block_size):
                hasher.update(chunk)
    except Exception as e:
        print(f"[!] Failed to read {filepath}: {e}")
        return None
    return hasher.hexdigest()

def get_audio_tags(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if not audio:
            return None, None
        artist = audio.get("artist", [None])[0]
        title = audio.get("title", [None])[0]
        return artist or "", title or ""
    except Exception as e:
        print(f"[!] Error reading tags: {filepath} -> {e}")
        return "", ""

def is_fuzzy_match(tags1, tags2):
    artist1, title1 = tags1
    artist2, title2 = tags2
    if not (artist1 and title1 and artist2 and title2):
        return False
    artist_ratio = SequenceMatcher(None, artist1.lower(), artist2.lower()).ratio()
    title_ratio = SequenceMatcher(None, title1.lower(), title2.lower()).ratio()
    return artist_ratio > 0.85 and title_ratio > 0.85

def find_duplicates(source_dir, dupe_dir):
    seen_hashes = {}
    duplicates = []

    for root, _, files in os.walk(source_dir):
        for name in files:
            filepath = os.path.join(root, name)
            ext = os.path.splitext(filepath)[1].lower()
            if ext not in SUPPORTED_EXTS:
                continue

            print(f"[.] Scanning file: {filepath}")
            file_hash = compute_hash(filepath)
            if not file_hash:
                continue

            tags = get_audio_tags(filepath)
            matched = False

            for seen_file, seen_hash, seen_tags in seen_hashes.values():
                if file_hash == seen_hash:
                    print(f"[✓] Exact duplicate found (hash): {filepath}")
                    matched = True
                    break
                elif is_fuzzy_match(tags, seen_tags):
                    print(f"[~] Fuzzy duplicate found: {filepath} ~ {seen_file}")
                    matched = True
                    break

            if matched:
                duplicates.append(filepath)
            else:
                seen_hashes[filepath] = (filepath, file_hash, tags)

    print(f"\n[✔] Duplicate scan complete. {len(duplicates)} duplicates found.\n")

    if not os.path.exists(dupe_dir):
        os.makedirs(dupe_dir)

    for dup in duplicates:
        try:
            target = os.path.join(dupe_dir, os.path.basename(dup))
            print(f"[→] Moving duplicate to: {target}")
            shutil.move(dup, target)
        except Exception as e:
            print(f"[!] Failed to move {dup}: {e}")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python find_duplicates.py <source_directory> <duplicate_output_directory>")
        sys.exit(1)

    source = sys.argv[1]
    output = sys.argv[2]

    if not os.path.isdir(source):
        print(f"[!] Invalid source directory: {source}")
        sys.exit(1)

    find_duplicates(source, output)
