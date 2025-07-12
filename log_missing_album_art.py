import os
import sys
import json
from mutagen import File as MutagenFile

SUPPORTED_EXTS = [".mp3", ".flac", ".m4a", ".ogg", ".aac", ".wav"]

def has_embedded_artwork(filepath):
    audio = MutagenFile(filepath, easy=False)
    if audio is None:
        return False

    ext = os.path.splitext(filepath)[1].lower()

    try:
        if ext == ".mp3" and hasattr(audio, "tags"):
            return any(frame.FrameID == "APIC" for frame in audio.tags.values()) if audio.tags else False
        elif ext == ".flac":
            return bool(audio.pictures)
        elif ext == ".m4a":
            return "covr" in audio.tags if audio.tags else False
        elif ext == ".ogg":
            return False  # Assume external cover art for OGG
        elif ext == ".aac" or ext == ".wav":
            return False
    except Exception:
        return False

    return False

def extract_tags(filepath):
    audio = MutagenFile(filepath, easy=True)
    if not audio:
        return "Unknown Artist", "Unknown Album"
    artist = audio.get("artist", ["Unknown Artist"])[0]
    album = audio.get("album", ["Unknown Album"])[0]
    return artist.strip(), album.strip()

def scan_library_for_missing_art(root_dir):
    missing_art_entries = []

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext not in SUPPORTED_EXTS:
                continue

            fullpath = os.path.join(dirpath, fname)
            if not has_embedded_artwork(fullpath):
                artist, album = extract_tags(fullpath)
                missing_art_entries.append({
                    "file": fullpath,
                    "artist": artist,
                    "album": album
                })

    return missing_art_entries

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python log_missing_album_art.py <music_library_root> <output_json_file>")
        sys.exit(1)

    music_root = sys.argv[1]
    output_file = sys.argv[2]

    if not os.path.isdir(music_root):
        print(f"[✗] Invalid music library directory: {music_root}")
        sys.exit(1)

    print(f"[INFO] Scanning '{music_root}' for files missing embedded album art...")
    missing_data = scan_library_for_missing_art(music_root)
    print(f"[✓] Found {len(missing_data)} files missing album art.")

    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(missing_data, f, indent=2, ensure_ascii=False)

    print(f"[✓] Missing album art file list saved to '{output_file}'")
