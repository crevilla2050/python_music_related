# This script fetches and embeds album art into music files based on a JSON file listing missing art.
# It supports MP3, FLAC, and M4A formats, and can fetch cover art from MusicBrainz and Deezer.
# It also checks for sibling cover images in the same directory as the music file.
# The script uses the Mutagen library to handle audio file metadata and embedding.
# It can be run from the command line with a JSON file containing entries for each music file.
# Each entry should have a "file" key with the file path, and optionally "artist" and "album" keys.
# If no cover art is found, it logs the failure and continues processing the next entry.
# The script caches album art data to avoid redundant network requests for the same directory.
# It prints status messages to indicate success or failure for each file processed.

# fetch_and_embed_album_art.py

import os
import sys
import json
import requests
import base64
from mutagen import File
from mutagen.flac import Picture
from mutagen.id3 import ID3, APIC, error
from mutagen.mp4 import MP4Cover

SUPPORTED_EMBED_EXTS = [".mp3", ".flac", ".m4a"]
SUPPORTED_ALL_EXTS = SUPPORTED_EMBED_EXTS + [".ogg", ".aac", ".wav"]
COVER_FILENAMES = ["cover.jpg", "folder.jpg", "AlbumArtSmall.jpg"]

album_art_cache = {}

def fetch_cover_art_from_musicbrainz(artist, album):
    try:
        import musicbrainzngs
        musicbrainzngs.set_useragent("AlbumArtFetcher", "1.0", "you@example.com")
        result = musicbrainzngs.search_releases(artist=artist, release=album, limit=1)
        if result['release-list']:
            release_id = result['release-list'][0]['id']
            art = musicbrainzngs.get_image_front(release_id)
            return art
    except:
        return None
    return None

def fetch_cover_art_from_deezer(artist, album):
    try:
        query = f"{artist} {album}".replace(" ", "+")
        url = f"https://api.deezer.com/search/album?q={query}"
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if data["data"]:
                cover_url = data["data"][0]["cover_xl"]
                img_response = requests.get(cover_url, timeout=10)
                if img_response.status_code == 200:
                    return img_response.content
    except:
        pass
    return None

def find_sibling_cover(directory):
    for file in os.listdir(directory):
        lower = file.lower()
        if any(name in lower for name in COVER_FILENAMES):
            with open(os.path.join(directory, file), "rb") as img:
                return img.read()
    return album_art_cache.get(directory)

def embed_art(filepath, image_data):
    ext = os.path.splitext(filepath)[1].lower()
    audio = File(filepath, easy=False)
    if not audio:
        return False

    if ext == ".mp3":
        if not isinstance(audio.tags, ID3):
            audio.add_tags()
        audio.tags.add(APIC(
            encoding=3,
            mime="image/jpeg",
            type=3,
            desc="Cover",
            data=image_data
        ))
    elif ext == ".flac":
        pic = Picture()
        pic.data = image_data
        pic.type = 3
        pic.mime = "image/jpeg"
        audio.add_picture(pic)
    elif ext == ".m4a":
        cover = MP4Cover(image_data, imageformat=MP4Cover.FORMAT_JPEG)
        audio.tags["covr"] = [cover]
    else:
        # fallback: write sidecar image
        image_path = os.path.join(os.path.dirname(filepath), "cover.jpg")
        with open(image_path, "wb") as f:
            f.write(image_data)
        return True

    audio.save()
    return True

def process_entry(entry):
    filepath = entry.get("file")
    artist = entry.get("artist")
    album = entry.get("album")

    if not os.path.exists(filepath):
        print(f"[✗] File not found: {filepath}")
        return

    directory = os.path.dirname(filepath)

    image_data = find_sibling_cover(directory)

    if not image_data:
        print(f"[•] Looking online for cover art for: {artist} - {album}")
        image_data = fetch_cover_art_from_musicbrainz(artist, album)

    if not image_data:
        image_data = fetch_cover_art_from_deezer(artist, album)

    if not image_data:
        print(f"[✗] No cover found for: {filepath}")
        return

    album_art_cache[directory] = image_data

    success = embed_art(filepath, image_data)
    if success:
        print(f"[✓] Embedded cover for: {filepath}")
    else:
        print(f"[✗] Failed to embed cover: {filepath}")

def main(json_path):
    with open(json_path, encoding="utf-8") as f:
        entries = json.load(f)

    if isinstance(entries, list) and all(isinstance(e, str) for e in entries):
        # Legacy format
        entries = [{"file": path, "artist": "", "album": ""} for path in entries]

    for entry in entries:
        process_entry(entry)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python fetch_and_embed_album_art.py <missing_art.json>")
        sys.exit(1)

    main(sys.argv[1])
