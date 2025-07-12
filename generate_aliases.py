import os
import unicodedata
import json
import sys

OUTPUT_JSON = "artist_album_aliases.json"

def normalize_name(name):
    """Normalize a string for comparison (remove accents, lowercase, strip special chars)."""
    name = name.lower()
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    name = ''.join(c for c in name if c.isalnum() or c.isspace()).strip()
    return name

def scan_music_library(root_path):
    artist_aliases = {}
    album_aliases = {}

    for dirpath, dirnames, filenames in os.walk(root_path):
        rel_path = os.path.relpath(dirpath, root_path)
        parts = rel_path.split(os.sep)

        if len(parts) == 1 and parts[0] not in ["Collections", "."]:
            # Top-level artist
            raw_artist = parts[0]
            norm_artist = normalize_name(raw_artist)
            if norm_artist not in artist_aliases:
                artist_aliases[norm_artist] = raw_artist

        if len(parts) == 2:
            # Artist/Album or Collections/Album
            raw_album = parts[1]
            norm_album = normalize_name(raw_album)
            if norm_album not in album_aliases:
                album_aliases[norm_album] = raw_album

    return {
        "artist_aliases": artist_aliases,
        "album_aliases": album_aliases
    }

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python generate_aliases.py <organized_music_root>")
        sys.exit(1)

    root_path = sys.argv[1]
    if not os.path.isdir(root_path):
        print(f"[✗] Not a valid directory: {root_path}")
        sys.exit(1)

    data = scan_music_library(root_path)

    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    print(f"[✓] Alias data written to: {OUTPUT_JSON}")
    print(f"[✓] Found {len(data['artist_aliases'])} artist aliases and {len(data['album_aliases'])} album aliases.")
