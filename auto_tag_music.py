def log(message):
    # Append the log message to the log file and also print it to the console
    with open(LOG_FILE, "a", encoding="utf-8") as f:
        f.write(message + "\n")
    print(message)

def fetch_tags_from_musicbrainz(filepath):
    # Extract filename without extension
    filename = os.path.splitext(os.path.basename(filepath))[0]
    # Expect filenames in the format "Artist - Title"
    if '-' not in filename:
        return None
    # Split the filename into artist and title guesses
    artist_guess, title_guess = map(str.strip, filename.split('-', 1))
    try:
        # Query MusicBrainz for matching recordings
        result = musicbrainzngs.search_recordings(artist=artist_guess, recording=title_guess, limit=1)
        if result['recording-list']:
            rec = result['recording-list'][0]
            # Extract artist, title, and album information
            artist = rec['artist-credit'][0]['artist']['name']
            title = rec['title']
            album = rec['release-list'][0]['title'] if 'release-list' in rec else 'Unknown Album'
            return {
                "artist": artist,
                "album": album,
                "title": title,
                "tracknumber": "00",  # Default track number
                "compilation": "1" if len(set(a['artist']['name'] for a in rec['artist-credit'])) > 1 else "0"
            }
    except Exception as e:
        # Log any errors from MusicBrainz
        log(f"[!] MusicBrainz error for {filepath}: {e}")
    return None

def enrich_tags(filepath):
    try:
        # Open the audio file using mutagen
        audio = MutagenFile(filepath, easy=True)
        if not audio:
            log(f"[!] Unsupported or unreadable file: {filepath}")
            return False

        needs_update = False  # Track if any tag was updated

        # Read existing tags or fallback to empty strings
        artist = audio.get("artist", [""])[0].strip()
        title = audio.get("title", [""])[0].strip()
        album = audio.get("album", [""])[0].strip()

        # Normalize and update artist tag if needed
        if artist:
            normalized = normalize_artist_name(artist)
            if normalized != artist:
                log(f"[*] Normalizing artist: '{artist}' -> '{normalized}'")
                audio["artist"] = normalized
                needs_update = True

        # Normalize and update album tag if needed
        if album:
            normalized_album = normalize_album_name(album)
            if normalized_album != album:
                log(f"[*] Normalizing album: '{album}' -> '{normalized_album}'")
                audio["album"] = normalized_album
                needs_update = True

        # If any key tags are missing, try fetching from MusicBrainz
        if not artist or not title or not album:
            log(f"[*] Missing tags. Trying MusicBrainz for: {filepath}")
            mb_tags = fetch_tags_from_musicbrainz(filepath)
            if mb_tags:
                # Fill missing tags from MusicBrainz response
                for key, value in mb_tags.items():
                    if not audio.get(key):
                        audio[key] = value
                        needs_update = True
                # Normalize MusicBrainz artist and album if needed
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

        # If anything was changed, save the updated tags
        if needs_update:
            audio.save()
            log(f"[✓] Tags updated: {filepath}")
            return True
        else:
            log(f"[=] Tags already complete and normalized: {filepath}")
            return False

    except Exception as e:
        # Log any failure during processing
        log(f"[!] Failed to tag {filepath}: {e}")
        return False

def process_directory(root_dir):
    # Validate that the input is a directory
    if not os.path.isdir(root_dir):
        log(f"[✗] Not a directory: {root_dir}")
        return

    log(f"[INFO] Scanning: {root_dir}")
    # Recursively walk through all files in the directory
    for dirpath, _, filenames in os.walk(root_dir):
        for fname in filenames:
            filepath = os.path.join(dirpath, fname)
            ext = os.path.splitext(fname)[1].lower()
            # Process only supported audio files
            if ext in SUPPORTED_EXTS:
                enrich_tags(filepath)

# Script entry point
if __name__ == "__main__":
    # Require a directory path as argument
    if len(sys.argv) < 2:
        print("Usage: python auto_tag_music.py <directory>")
        sys.exit(1)

    # Start processing the given directory
    process_directory(sys.argv[1])
