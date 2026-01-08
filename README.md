# Pedro Organiza 🎵

**Pedro Organiza** is a backend-first music library organization and management system designed for large, messy, real-world music collections.

It focuses on **knowledge extraction, human-in-the-loop decision making, and reproducible actions**, rather than blindly renaming or deleting files.

This project is currently **work in progress**, with a strong emphasis on correctness, auditability, and flexibility.

---

## Philosophy

Pedro Organiza is built around a few core principles:

- **Knowledge before action**  
  All analysis happens first. Files are never modified until decisions are explicitly planned and approved.

- **Human-in-the-loop**  
  The system assists you; it does not override your judgment. Ambiguities are surfaced for review, not auto-fixed.

- **Database as the source of truth**  
  All decisions, plans, and states live in SQLite. The filesystem is treated as an execution target, not a database.

- **Idempotent & auditable**  
  You can stop, resume, revise, and replay operations safely.

---

## High-level Pipeline

Pedro Organiza follows a layered pipeline:

1. **Ingest & Knowledge (Layer 1)**
   - File discovery
   - Metadata extraction (tags)
   - SHA-256 hashing
   - Optional audio fingerprinting (Chromaprint)
   - Album art discovery (embedded & external)
   - Genre ingestion & normalization
   - All data stored in SQLite

2. **Analysis & Planning (Layer 2)**
   - Duplicate detection (hash, fingerprint, metadata)
   - Canonical file selection
   - Conflict identification
   - Action planning (move / archive / delete / skip)

3. **Execution (Layer 3)**
   - Apply planned actions
   - Filesystem operations are executed strictly from database plans
   - Soft deletes only (no irreversible operations)

4. **UI (Layer 4 – in progress)**
   - Review duplicates
   - Edit genres & tags
   - Select album art
   - Preview changes before applying
   - React-based frontend

---

## Project Structure

pedro-organiza/
├── backend/ # Core pipeline logic (ingest, plan, execute)
├── tools/ # CLIs, diagnostics, batch utilities
├── music-ui/ # React frontend (WIP)
├── README.md
├── LICENCE
└── requirements.txt


---

## Current Status

- Backend ingestion pipeline: **active development**
- Duplicate detection & planning: **functional**
- Genre normalization: **interactive CLI implemented**
- Album art discovery: **local (embedded & filesystem)**
- Internet metadata fetching: **planned**
- UI: **early prototype (React)**

Pedro Organiza is **not yet a packaged application**.  
It is currently intended for technical users comfortable with Python and SQLite.

---

## What Pedro Organiza Is NOT

- ❌ A one-click “organize my music” script  
- ❌ A music player  
- ❌ A destructive cleanup tool  

Pedro Organiza is a **library intelligence system**, not a magic wand.

---

## Future Plans (non-exhaustive)

- Internet metadata fetching (MusicBrainz, Cover Art Archive)
- Album art selection & normalization
- Soft tags & collections
- Playlist export (.m3u, iTunes XML)
- Fully interactive UI
- Cross-platform packaging

---

## License

Pedro Organiza is released under the **MIT License**.  
See the `LICENCE` file for details.

---

## Contributing

The project is still stabilizing its core architecture.  
Contributions, ideas, and discussions are welcome — especially around:

- Metadata normalization
- Duplicate resolution strategies
- UX for large music libraries

More contribution guidelines will follow.

---

*Pedro Organiza is built for people who care about their music libraries.*
