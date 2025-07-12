import os
import sys
import json
import unicodedata
from mutagen import File as MutagenFile

SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']

def normalize_name(name):
    """
    Normalize a string for comparison:
    - lowercase
    - unicode NFKD normalization
    - remove accents
    - keep only alphanumeric and spaces
    """
    name = name.lower()
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    name = ''.join(c for c in name if c.isalnum() or c.isspace()).strip()
    return name

def scan_music_files(root_path):
    """
    Walk through music files under root_path,
    read artist and album tags, collect variants in dictionaries.
    Returns two dicts:
      artist_variants: {normalized_name: set(actual_names)}
      album_variants: {normalized_name: set(actual_names)}
    """
    artist_variants = {}
    album_variants = {}

    for dirpath, _, files in os.walk(root_path):
        for fname in files:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in SUPPORTED_EXTS:
                continue
            filepath = os.path.join(dirpath, fname)
            try:
                audio = MutagenFile(filepath, easy=True)
                if not audio:
                    continue
                # Artist
                if "artist" in audio and audio["artist"]:
                    artist_raw = audio["artist"][0].strip()
                    norm_artist = normalize_name(artist_raw)
                    artist_variants.setdefault(norm_artist, set()).add(artist_raw)
                # Album
                if "album" in audio and audio["album"]:
                    album_raw = audio["album"][0].strip()
                    norm_album = normalize_name(album_raw)
                    album_variants.setdefault(norm_album, set()).add(album_raw)
            except Exception as e:
                print(f"[!] Failed reading {filepath}: {e}", file=sys.stderr)
    return artist_variants, album_variants

def scan_folder_structure(root_path):
    """
    Walk folder structure and collect artist and album folder names as variants.
    Assumes structure: /Artist/Album or /Collections/Album
    Adds these names into variants dicts.
    """
    artist_variants = {}
    album_variants = {}

    for dirpath, dirnames, _ in os.walk(root_path):
        rel_path = os.path.relpath(dirpath, root_path)
        parts = rel_path.split(os.sep)

        # Ignore root dir itself
        if rel_path == ".":
            continue

        # Top level artist folder (e.g. /Artist)
        if len(parts) == 1 and parts[0].lower() != "collections":
            artist_raw = parts[0]
            norm_artist = normalize_name(artist_raw)
            artist_variants.setdefault(norm_artist, set()).add(artist_raw)

        # Artist/Album or Collections/Album
        if len(parts) == 2:
            # Add album name
            album_raw = parts[1]
            norm_album = normalize_name(album_raw)
            album_variants.setdefault(norm_album, set()).add(album_raw)
            # If not collections, add artist too
            if parts[0].lower() != "collections":
                artist_raw = parts[0]
                norm_artist = normalize_name(artist_raw)
                artist_variants.setdefault(norm_artist, set()).add(artist_raw)

    return artist_variants, album_variants

def build_canonical_aliases(variants_dict):
    """
    From variants dict {norm_name: set(actual_names)},
    create aliases dict {variant: canonical_name}
    where canonical_name is the longest variant for that norm_name.
    """
    aliases = {}
    for norm, variants in variants_dict.items():
        # Choose longest variant as canonical name
        canonical = sorted(variants, key=len)[-1]
        for alias in variants:
            if alias != canonical:
                aliases[alias] = canonical
    return aliases

def merge_variants(dict1, dict2):
    """
    Merge two variants dicts: {norm_name: set(actual_names)}
    """
    merged = dict1.copy()
    for norm, variants in dict2.items():
        if norm in merged:
            merged[norm].update(variants)
        else:
            merged[norm] = set(variants)
    return merged

def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <music_root_dir> [output_json]")
        sys.exit(1)

    root_path = sys.argv[1]
    if not os.path.isdir(root_path):
        print(f"[✗] Not a valid directory: {root_path}")
        sys.exit(1)

    output_file = sys.argv[2] if len(sys.argv) > 2 else "artist_album_aliases.json"

    print("[*] Scanning music files for tags...")
    file_artist_variants, file_album_variants = scan_music_files(root_path)

    print("[*] Scanning folder structure for artist and album names...")
    folder_artist_variants, folder_album_variants = scan_folder_structure(root_path)

    print("[*] Merging variants...")
    artist_variants = merge_variants(file_artist_variants, folder_artist_variants)
    album_variants = merge_variants(file_album_variants, folder_album_variants)

    print("[*] Building canonical aliases...")
    artist_aliases = build_canonical_aliases(artist_variants)
    album_aliases = build_canonical_aliases(album_variants)

    combined = {
        "artist_aliases": artist_aliases,
        "album_aliases": album_aliases
    }

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(combined, f, indent=2, ensure_ascii=False)

    print(f"[✓] Alias data written to: {output_file}")
    print(f"[✓] Found {len(artist_aliases)} artist aliases and {len(album_aliases)} album aliases.")

if __name__ == "__main__":
    main()
