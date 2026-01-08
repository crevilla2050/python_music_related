#!/usr/bin/env python3

import os
import sys
from mutagen import File as MutagenFile
import musicbrainzngs

from backend.i18n.messages import msg

LOG_FILE = "auto_tag_music.log"

SUPPORTED_EXTS = {".mp3", ".flac", ".ogg", ".m4a", ".wav"}

# ---------------- LOGGING ----------------

def log(message_key, **kwargs):
    """
    Log a translated message to file and console.
    """
    message = msg(message_key).format(**kwargs)
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    print(message)


# ---------------- MUSICBRAINZ ----------------

def fetch_tags_from_musicbrainz(filepath):
    filename = os.path.splitext(os.path.basename(filepath))[0]

    if "-" not in filename:
        return None

    artist_guess, title_guess = map(str.strip, filename.split("-", 1))

    try:
        result = musicbrainzngs.search_recordings(
            artist=artist_guess,
            recording=title_guess,
            limit=1
        )

        if result.get("recording-list"):
            rec = result["recording-list"][0]

            artist = rec["artist-credit"][0]["artist"]["name"]
            title = rec["title"]
            album = (
                rec["release-list"][0]["title"]
                if "release-list" in rec
                else msg("UNKNOWN_ALBUM")
            )

            return {
                "artist": artist,
                "album": album,
                "title": title,
                "tracknumber": "00",
                "compilation": "1"
                if len(set(a["artist"]["name"] for a in rec["artist-credit"])) > 1
                else "0",
            }

    except Exception as e:
        log(
            "MB_ERROR",
            filepath=filepath,
            error=str(e)
        )

    return None


# ---------------- TAG ENRICHMENT ----------------

def enrich_tags(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if not audio:
            log("UNSUPPORTED_FILE", filepath=filepath)
            return False

        needs_update = False

        artist = audio.get("artist", [""])[0].strip()
        title = audio.get("title", [""])[0].strip()
        album = audio.get("album", [""])[0].strip()

        if not artist or not title or not album:
            log("MISSING_TAGS", filepath=filepath)
            mb_tags = fetch_tags_from_musicbrainz(filepath)

            if mb_tags:
                for key, value in mb_tags.items():
                    if not audio.get(key):
                        audio[key] = value
                        needs_update = True

        if needs_update:
            audio.save()
            log("TAGS_UPDATED", filepath=filepath)
            return True

        log("TAGS_ALREADY_OK", filepath=filepath)
        return False

    except Exception as e:
        log(
            "TAGGING_FAILED",
            filepath=filepath,
            error=str(e)
        )
        return False


# ---------------- DIRECTORY WALK ----------------

def process_directory(root_dir):
    if not os.path.isdir(root_dir):
        log("NOT_A_DIRECTORY", path=root_dir)
        return

    log("SCANNING_DIRECTORY", path=root_dir)

    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            ext = os.path.splitext(fname)[1].lower()
            if ext in SUPPORTED_EXTS:
                enrich_tags(os.path.join(dirpath, fname))


# ---------------- CLI ----------------

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(msg("AUTO_TAG_USAGE"))
        sys.exit(1)

    process_directory(sys.argv[1])
