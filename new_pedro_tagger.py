"""
new_pedro_tagger.py

Pedro v2 (NO AcoustID key required):
- Local fingerprint awareness (already in DB)
- MusicBrainz text-based enrichment
- Honest confidence reporting
- Safe for FastAPI integration
"""

import os
import logging
import musicbrainzngs

# ---------------- logging ----------------
log = logging.getLogger("pedro")

# ---------------- MusicBrainz config ----------------
musicbrainzngs.set_useragent(
    "MusicConsolidator",
    "2.0",
    "you@example.com"
)

# ---------------- helpers ----------------
def _guess_from_filename(filepath: str):
    name = os.path.splitext(os.path.basename(filepath))[0]
    if "-" not in name:
        return None, None
    artist, title = map(str.strip, name.split("-", 1))
    return artist, title


def _search_musicbrainz(artist: str, title: str):
    try:
        result = musicbrainzngs.search_recordings(
            artist=artist,
            recording=title,
            limit=1
        )

        recs = result.get("recording-list", [])
        if not recs:
            return None

        rec = recs[0]
        artist_name = rec["artist-credit"][0]["artist"]["name"]
        title_name = rec["title"]
        album_name = None

        if rec.get("release-list"):
            album_name = rec["release-list"][0]["title"]

        return {
            "artist": artist_name,
            "title": title_name,
            "album": album_name
        }

    except Exception:
        log.exception("MusicBrainz search failed")
        return None


# ---------------- public API ----------------
def pedro_enrich_file(filepath: str, artist_hint=None, title_hint=None) -> dict:
    """
    Pedro enrichment (suggestions only, NEVER authoritative)
    """

    if not os.path.isfile(filepath):
        return {
            "source": "pedro",
            "confidence": 0,
            "reason": "file_not_found"
        }

    # Priority 1: explicit user input
    if artist_hint and title_hint:
        tags = _search_musicbrainz(artist_hint, title_hint)
        if tags:
            return {
                "source": "musicbrainz",
                "method": "manual_hints",
                "confidence": 0.75,
                "suggested": {
                    "artist": tags.get("artist"),
                    "album": tags.get("album"),
                    "title": tags.get("title"),
                    "track": None
                }
            }

    # Priority 2: filename guess
    artist_guess, title_guess = _guess_from_filename(filepath)
    if artist_guess and title_guess:
        tags = _search_musicbrainz(artist_guess, title_guess)
        if tags:
            return {
                "source": "musicbrainz",
                "method": "filename_guess",
                "confidence": 0.60,
                "suggested": {
                    "artist": tags.get("artist"),
                    "album": tags.get("album"),
                    "title": tags.get("title"),
                    "track": None
                }
            }

    return {
        "source": "musicbrainz",
        "confidence": 0,
        "reason": "no_match"
    }
