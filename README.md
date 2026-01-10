# Pedro Organiza 🎵

Pedro Organiza is a backend-first, CLI-driven music library knowledge ingestion and organization system designed for large, messy, real-world music collections.

Instead of blindly renaming, deleting, or “fixing” files, Pedro analyzes your library first, builds a complete and inspectable SQLite knowledge base, and lets you decide what happens next.

Knowledge first. Actions later. Always auditable.

Pedro Organiza is intentionally conservative, transparent, and reproducible. It is built for people who care about their music libraries and want to understand them before changing anything.

--------------------------------------------------------------------

WHY PEDRO ORGANIZA EXISTS

Most music organization tools assume:
- small libraries
- perfect or near-perfect tags
- one-click automation
- irreversible actions

Real collections rarely fit that model.

Pedro Organiza is designed for:
- libraries with tens of thousands of tracks
- inconsistent or conflicting metadata
- multiple versions of the same recordings
- long-running analysis jobs
- users who want control, traceability, and safety

Pedro does not try to guess what you want.
It gives you the information needed to make correct decisions yourself.

--------------------------------------------------------------------

CORE PRINCIPLES

Knowledge Before Action
All analysis happens first.
Files are never modified during ingestion or analysis.

Human-in-the-Loop
Ambiguities are surfaced for review instead of auto-fixed.
Pedro assists you; it does not override your judgment.

Database as Source of Truth
All knowledge, decisions, plans, and states live in SQLite.
The filesystem is treated as an execution target, not a database.

Deterministic & Auditable
You can stop, resume, inspect, revise, and replay operations safely.
Every step is reproducible.

Low-Resource Friendly
Pedro is designed to run reliably on laptops, NAS boxes, home servers,
and Raspberry Pi–class hardware.
No aggressive parallelism, no hidden background jobs.

--------------------------------------------------------------------

HIGH-LEVEL PIPELINE

Pedro Organiza follows a layered pipeline by intent, not convenience.

Layer 1 – Ingest & Knowledge (Stable)

This is the foundation of Pedro and the focus of the current CLI release.

- Recursive file discovery
- Metadata extraction (audio tags)
- SHA-256 hashing
- Optional audio fingerprinting (Chromaprint)
- Album art discovery (embedded and filesystem)
- Genre ingestion
- Recommended canonical paths
- All results stored in SQLite

This layer builds a complete, inspectable knowledge database without
modifying your files.

Layer 2 – Analysis & Planning (Partial)

- Duplicate detection (hash, fingerprint, metadata)
- Conflict identification
- Canonical file selection
- Action planning (move, archive, skip)

Logic exists, but normalization and grouping are still evolving.

Layer 3 – Execution (Conservative by Design)

- Applies only explicitly planned actions
- Filesystem changes are driven strictly from database state
- No irreversible deletes

Layer 4 – UI (Early Prototype)

- React-based frontend (work in progress)
- Review duplicates
- Edit genres and tags
- Preview actions before execution

--------------------------------------------------------------------

WHAT PEDRO CAN DO TODAY (CLI)

The current CLI release focuses on safe ingestion and knowledge extraction:

- Build complete SQLite databases from large music libraries
- Inspect metadata, hashes, fingerprints, and album art
- Prepare libraries for later organization steps
- Run long analysis jobs safely and deterministically

Pedro does not automatically reorganize your library.
That is a deliberate design choice.

--------------------------------------------------------------------

WHAT PEDRO ORGANIZA IS NOT

- Not a one-click “organize my music” script
- Not a music player
- Not a destructive cleanup tool
- Not a tag-only fixer

Pedro Organiza is a library intelligence system, not a magic wand.

--------------------------------------------------------------------

INSTALLATION (CLI)

Pedro Organiza is distributed as a command-line tool.

The installation process is intentionally conservative:
- system Python is never modified
- dependencies are installed locally
- no global packages are required

--------------------------------------------------------------------

REQUIREMENTS

- Python 3.9 or newer
- Read access to your music library
- Write access to the Pedro project directory
- Disk space for SQLite databases (can be large for big libraries)

--------------------------------------------------------------------

QUICK INSTALL (RECOMMENDED)

LINUX / MACOS

From the project root:

chmod +x install.sh
./install.sh

The installer will:
1. Check that python3 is available
2. Verify Python version (3.9+)
3. Create a local virtual environment (./venv)
4. Install Pedro and all dependencies inside that environment
5. Leave your system Python untouched

After installation:

source venv/bin/activate
pedro status

--------------------------------------------------------------------

WINDOWS

1. Install Python 3.9+ from https://www.python.org
   Ensure “Add Python to PATH” is enabled during installation.

2. Open Command Prompt or PowerShell

3. From the project root:

install.bat

After installation:

venv\Scripts\activate
pedro status

--------------------------------------------------------------------

COMMON SCENARIOS & TROUBLESHOOTING

Python Not Found (Linux/macOS)
Install Python using your system package manager, then re-run install.sh.

Python Version Too Old
Pedro requires Python 3.9 or newer.
Installing a newer Python alongside system Python is safe.

Permission Errors When Scanning Music
Pedro requires read access to your music files and write access to its
database directory.
Avoid running Pedro as root unless absolutely necessary.

SQLite “Database Is Locked”
Do not run multiple Pedro instances on the same database.
SQLite allows one writer at a time by design.

Fingerprinting Issues (ffmpeg / chromaprint)
Audio fingerprinting is optional.
If needed, run without --with-fingerprint.
Fingerprinting can be added later in a separate run.

--------------------------------------------------------------------

VERIFYING THE INSTALLATION

After activating the virtual environment:

pedro status

If this runs without errors, Pedro is installed correctly.

--------------------------------------------------------------------

EXAMPLE INGEST RUN

pedro run backend/consolidate_music.py --
  --src "/path/to/music"
  --lib "/path/to/organized/library"
  --db "my_library.sqlite"
  --with-fingerprint
  --search-covers
  --progress

This builds a complete SQLite knowledge database without modifying files.

--------------------------------------------------------------------

PROJECT STRUCTURE

pedro-organiza/
├── backend/
├── cli/
├── databases/
├── music-ui/
├── tests/
├── install.sh
├── install.bat
├── README.md
└── LICENSE

--------------------------------------------------------------------

CURRENT STATUS

- CLI ingestion pipeline: stable
- Duplicate detection & planning: partial
- Normalization & alias detection: in progress
- UI: early prototype

Pedro Organiza is best suited for users comfortable with Python and SQLite.

--------------------------------------------------------------------

LICENSE

Pedro Organiza is released under the MIT License.
See the LICENSE file for details.

--------------------------------------------------------------------

CONTRIBUTING

Pedro Organiza is stabilizing its core architecture.

Contributions, discussions, and feedback are welcome, especially around:
- metadata normalization rules
- duplicate resolution strategies
- UX for large music libraries
- real-world edge cases

Formal contribution guidelines will follow.

--------------------------------------------------------------------

Pedro Organiza is built for people who care about their music libraries
and want to understand them before changing them.

## License

Pedro Organiza is licensed for **personal, educational, and non-commercial use**
under the Pedro Organiza Non-Commercial License.

Commercial use requires a separate commercial license.
Please contact the author for details.


See the LICENSE file for the full license text.