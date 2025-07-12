# auto_tag_music.py
import os
import sys
import unicodedata
import musicbrainzngs
from mutagen import File as MutagenFile
from mutagen.easyid3 import EasyID3

SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']

musicbrainzngs.set_useragent("MusicTagger", "1.0", "carlos.revilla.m@gmail.com")

# Known aliases mapping for artist normalization
ARTIST_ALIASES = {
    "vicente fernandez": "Vicente Fernández",
    "vicente fernández": "Vicente Fernández",
    "v. fernandez": "Vicente Fernández",
    "v fernandez": "Vicente Fernández",
    "cafe tacuba": "Café Tacvba",
    "cafe tacvba": "Café Tacvba",
    "café tacuba": "Café Tacvba",
    "café tacvba": "Café Tacvba",
    "cartel de santa": "Cartel De Santa",
    "c. santa": "Cartel De Santa",
    # Add more mappings as needed
}

ALBUM_ALIASES = {
    # Add album-level normalization if needed
    # "greatest hits": "Greatest Hits"
}

LOG_FILE = "auto_tag_music.log"

def normalize_name(name):
    name = name.lower()
    name = unicodedata.normalize('NFKD', name)
    name = ''.join(c for c in name if not unicodedata.combining(c))
    name = ''.join(c for c in name if c.isalnum() or c.isspace()).strip()
    return name

def normalize_artist_name(name):
    clean = normalize_name(name)
    return ARTIST_ALIASES.get(clean, name.title())

def normalize_album_name(name):
    clean = normalize_name(name)
    return ALBUM_ALIASES.get(clean, name.title())

def log(message):
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    print(message)

def fetch_tags_from_musicbrainz(filepath):
    filename = os.path.splitext(os.path.basename(filepath))[0]
    if '-' not in filename:
        return None
    artist_guess, title_guess = map(str.strip, filename.split('-', 1))
    try:
        result = musicbrainzngs.search_recordings(artist=artist_guess, recording=title_guess, limit=1)
        if result['recording-list']:
            rec = result['recording-list'][0]
            artist = rec['artist-credit'][0]['artist']['name']
            title = rec['title']
            album = rec['release-list'][0]['title'] if 'release-list' in rec else 'Unknown Album'
            return {
                "artist": artist,
                "album": album,
                "title": title,
                "tracknumber": "00",
                "compilation": "1" if len(set(a['artist']['name'] for a in rec['artist-credit'])) > 1 else "0"
            }
    except Exception as e:
        log(f"[!] MusicBrainz error for {filepath}: {e}")
    return None

def enrich_tags(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if not audio:
            log(f"[!] Unsupported or unreadable file: {filepath}")
            return False

        needs_update = False
        artist = audio.get("artist", [""])[0].strip()
        title = audio.get("title", [""])[0].strip()
        album = audio.get("album", [""])[0].strip()

        if artist:
            normalized = normalize_artist_name(artist)
            if normalized != artist:
                log(f"[*] Normalizing artist: '{artist}' -> '{normalized}'")
                audio["artist"] = normalized
                needs_update = True

        if album:
            normalized_album = normalize_album_name(album)
            if normalized_album != album:
                log(f"[*] Normalizing album: '{album}' -> '{normalized_album}'")
                audio["album"] = normalized_album
                needs_update = True

        if not artist or not title or not album:
            log(f"[*] Missing tags. Trying MusicBrainz for: {filepath}")
            mb_tags = fetch_tags_from_musicbrainz(filepath)
            if mb_tags:
                for key, value in mb_tags.items():
                    if not audio.get(key):
                        audio[key] = value
                        needs_update = True
                if mb_tags.get("artist"):
                    normalized = normalize_artist_name(mb_tags["artist"])
                    if normalized != mb_tags["artist"]:
                        log(f"[*] Normalizing MusicBrainz artist: '{mb_tags['artist']}' -> '{normalized}'")
                        audio["artist"] = normalized
                        needs_update = True
                if mb_tags.get("album"):
                    normalized_album = normalize_album_name(mb_tags["album"])
                    if normalized_album != mb_tags["album"]:
                        log(f"[*] Normalizing MusicBrainz album: '{mb_tags['album']}' -> '{normalized_album}'")
                        audio["album"] = normalized_album
                        needs_update = True

        if needs_update:
            audio.save()
            log(f"[✓] Tags updated: {filepath}")
            return True
        else:
            log(f"[=] Tags already complete and normalized: {filepath}")
            return False

    except Exception as e:
        log(f"[!] Failed to tag {filepath}: {e}")
        return False

def process_directory(root_dir):
    if not os.path.isdir(root_dir):
        log(f"[✗] Not a directory: {root_dir}")
        return

    log(f"[INFO] Scanning: {root_dir}")
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            filepath = os.path.join(dirpath, fname)
            ext = os.path.splitext(fname)[1].lower()
            if ext in SUPPORTED_EXTS:
                enrich_tags(filepath)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python auto_tag_music.py <directory>")
        sys.exit(1)

    process_directory(sys.argv[1])
