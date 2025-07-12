import os
import shutil
import sys
import logging
from mutagen import File as MutagenFile
from mutagen.mp3 import HeaderNotFoundError

SOURCE_DIR = sys.argv[1] if len(sys.argv) > 1 else "."
DEST_DIR = sys.argv[2] if len(sys.argv) > 2 else "./Organizadas"
LOG_DIR = DEST_DIR
LOG_FILE = os.path.join(LOG_DIR, "organize_log.txt")
BROKEN_FILE_LOG = os.path.join(LOG_DIR, "broken_files.txt")

os.makedirs(DEST_DIR, exist_ok=True)

logging.basicConfig(filename=LOG_FILE, level=logging.INFO, format="%(message)s")

def get_audio_tags(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if audio is None or not audio.tags:
            return None
        return {
            "artist": audio.get("artist", ["Unknown Artist"])[0],
            "album": audio.get("album", ["Unknown Album"])[0],
            "title": audio.get("title", [os.path.basename(filepath)])[0],
            "tracknumber": audio.get("tracknumber", [""])[0].split("/")[0]
        }
    except HeaderNotFoundError:
        raise
    except Exception as e:
        raise RuntimeError(f"Failed to read tags: {e}")

def organize_file(filepath):
    try:
        tags = get_audio_tags(filepath)
        if not tags:
            raise ValueError("Missing tags")

        artist = sanitize(tags["artist"])
        album = sanitize(tags["album"])
        title = sanitize(tags["title"])
        track = tags["tracknumber"].zfill(2) if tags["tracknumber"].isdigit() else ""

        new_dir = os.path.join(DEST_DIR, artist, album)
        os.makedirs(new_dir, exist_ok=True)

        filename = f"{track} - {artist} - {title}{os.path.splitext(filepath)[1]}"
        new_path = os.path.join(new_dir, filename)

        logging.info(f"[+] Mapped: {filepath} --> {new_path}")
        shutil.move(filepath, new_path)

        return new_path

    except (HeaderNotFoundError, RuntimeError, ValueError) as err:
        with open(BROKEN_FILE_LOG, "a") as broken_log:
            broken_log.write(f"[!] Skipped: {filepath} — {err}\n")
        return None

def sanitize(name):
    return "".join(c for c in name if c.isalnum() or c in " _-").strip()

def scan_and_organize(directory):
    for root, _, files in os.walk(directory):
        for file in files:
            filepath = os.path.join(root, file)
            if not file.lower().endswith((".mp3", ".flac", ".ogg", ".m4a", ".wav")):
                continue
            organize_file(filepath)

def main():
    print(f"[✓] Scanning: {SOURCE_DIR}")
    print(f"[✓] Moving files to: {DEST_DIR}")
    scan_and_organize(SOURCE_DIR)
    print("[✓] Done. See log files for details.")

if __name__ == "__main__":
    main()

