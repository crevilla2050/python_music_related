# auto_tag_music.py
import os
import sys
import json
import unicodedata
import musicbrainzngs
from mutagen import File as MutagenFile

SUPPORTED_EXTS = ['.mp3', '.flac', '.wav', '.m4a', '.ogg', '.aac']

musicbrainzngs.set_useragent("MusicTagger", "1.0", "yourmail@example.com")

# Known aliases mapping for artist normalization
ARTIST_ALIASES = {
   # Add more mappings as needed
}

ALBUM_ALIASES = {
    # Add album-level normalization if needed
    # "greatest hits": "Greatest Hits"
}

ALIAS_FILE = "artist_album_aliases.json"
LOG_FILE = "auto_tag_music.log"

musicbrainzngs.set_useragent("MusicTagger", "1.0", "yourmail@example.com")

# Load alias maps from JSON
# Define a function called load_aliases that takes a path as an argument
def load_aliases(path):
    # Check if the path exists
    if os.path.exists(path):
        # Open the file at the given path in read mode with utf-8 encoding
        with open(path, "r", encoding="utf-8") as f:
            # Load the data from the file
            data = json.load(f)
            # Return the artist_aliases and album_aliases from the data
            return data.get("artist_aliases", {}), data.get("album_aliases", {})
    # If the path does not exist, return empty dictionaries
    return {}, {}

ARTIST_ALIASES, ALBUM_ALIASES = load_aliases(ALIAS_FILE)

def normalize_name(name):
    # Convert the name to lowercase
    name = name.lower()
    # Normalize the name to remove any accents or diacritics
    name = unicodedata.normalize('NFKD', name)
    # Remove any combining characters
    name = ''.join(c for c in name if not unicodedata.combining(c))
    # Remove any non-alphanumeric characters and spaces
    name = ''.join(c for c in name if c.isalnum() or c.isspace()).strip()
    # Return the normalized name
    return name

# Define a function called normalize_artist_name that takes in a parameter called name
def normalize_artist_name(name):
    # Call the normalize_name function and assign the result to the variable normalized
    normalized = normalize_name(name)
    # Return the value of the ARTIST_ALIASES dictionary with the key of normalized, or the title of name if the key is not found
    return ARTIST_ALIASES.get(normalized, name.title())

def normalize_album_name(name):
    normalized = normalize_name(name)
    return ALBUM_ALIASES.get(normalized, name.title())

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
