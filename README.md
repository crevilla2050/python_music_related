# 🎵 Python Music Tools

A collection of Python scripts to help you manage, clean up, and organize your local music library.

This project was built to solve common problems like:
- Finding duplicate music files
- Generating consistent artist and album name aliases
- Organizing messy music folders using metadata and filename analysis

## 📦 Scripts Included

### `music_manager.py`

Combined script for:

- 🔍 Scanning music files and folder structure
- 🧠 Detecting fuzzy duplicates (via file hashes, tags, filenames)
- 📚 Building artist and album aliases using tag normalization and fuzzy matching

It outputs two useful JSON files:
- `artist_album_aliases.json` — canonical mapping of artist/album name variants
- `duplicates.json` — list of detected duplicate files for review/removal

## 🛠 Requirements

- Python 3.7+
- [`mutagen`](https://mutagen.readthedocs.io/en/latest/)

Install dependencies using pip:

```bash
pip install mutagen
🚀 Usage
python music_manager.py <music_folder> --mode [aliases|duplicates|all] [--verbose]
Examples:

# Full scan with verbose output
python music_manager.py /mnt/my_music --mode all --verbose

# Just detect duplicates
python music_manager.py /mnt/my_music --mode duplicates

# Just generate artist/album aliases
python music_manager.py /mnt/my_music --mode aliases
🔍 How It Works
Normalizes tag values and filenames (removes accents, symbols, common suffixes)

Groups similar values using fuzzy string matching

Detects duplicates by comparing:

SHA-256 hashes

Artist/album/title metadata

File name similarity

📁 Output Files
artist_album_aliases.json:

json
{
  "artist_aliases": {
    "The Beatles": "The Beatles",
    "beatles": "The Beatles",
    "Beatles (Remastered)": "The Beatles"
  },
  "album_aliases": {
    "abbey road": "Abbey Road"
  }
}
duplicates.json:

json
[
  [
    "/music/Beatles - Hey Jude.mp3",
    "/downloads/Beatles - Hey Jude (1).mp3"
  ],
  ...
]
🧪 Tested On
Linux Mint 22.1

Python 3.10

Local music folders with various file formats and naming schemes

📜 License
MIT License

🤝 Contributing
Feel free to open issues or submit pull requests to improve functionality, fix bugs, or suggest new tools!
