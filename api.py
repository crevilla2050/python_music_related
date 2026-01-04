"""
api.py

FastAPI backend for Music Consolidation UI
"""

import os
import sqlite3
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from new_pedro_tagger import pedro_enrich_file

# ---------------- config ----------------
DB_PATH = os.environ.get("MUSIC_DB", "music_consolidation.db")

# ---------------- app ----------------
app = FastAPI(title="Music Consolidation API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # dev
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------- helpers ----------------
def utcnow():
    return datetime.now(timezone.utc).isoformat()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

# ---------------- models ----------------
class FileUpdate(BaseModel):
    id: int
    artist: str | None = None
    album: str | None = None
    title: str | None = None
    recommended_path: str | None = None
    pedro_status: str | None = None
    pedro_notes: str | None = None

# ---------------- endpoints ----------------

@app.get("/files")
def list_files(
    action: str | None = Query(default=None),
    status: str | None = Query(default=None),
    limit: int = Query(default=200, le=1000),
):
    conn = get_db()
    c = conn.cursor()

    query = "SELECT * FROM files"
    where = []
    params = []

    if action:
        where.append("action = ?")
        params.append(action)

    if status:
        where.append("status = ?")
        params.append(status)

    if where:
        query += " WHERE " + " AND ".join(where)

    query += " ORDER BY id LIMIT ?"
    params.append(limit)

    c.execute(query, params)
    rows = [dict(r) for r in c.fetchall()]
    conn.close()
    return rows


@app.post("/files/update")
def update_files(updates: List[FileUpdate]):
    if not updates:
        return {"updated": 0}

    conn = get_db()
    c = conn.cursor()

    for u in updates:
        c.execute(
            """
            UPDATE files
            SET
                artist = COALESCE(?, artist),
                album = COALESCE(?, album),
                title = COALESCE(?, title),
                recommended_path = COALESCE(?, recommended_path),
                pedro_status = COALESCE(?, pedro_status),
                pedro_notes = COALESCE(?, pedro_notes),
                pedro_last_run = COALESCE(?, pedro_last_run),
                last_update = ?
            WHERE id = ?
            """,
            (
                u.artist,
                u.album,
                u.title,
                u.recommended_path,
                u.pedro_status,
                u.pedro_notes,
                utcnow() if u.pedro_status else None,
                utcnow(),
                u.id,
            ),
        )

    conn.commit()
    conn.close()

    return {"updated": len(updates)}


@app.get("/audio/{file_id}")
def stream_audio(file_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT original_path FROM files WHERE id = ?", (file_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "File not found")

    path = row["original_path"]
    if not os.path.isfile(path):
        raise HTTPException(404, "Audio file missing")

    mime, _ = mimetypes.guess_type(path)
    mime = mime or "audio/mpeg"

    def iterfile():
        with open(path, "rb") as f:
            while chunk := f.read(8192):
                yield chunk

    return StreamingResponse(iterfile(), media_type=mime)


@app.post("/pedro/enrich/{file_id}")
def pedro_enrich(file_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT original_path FROM files WHERE id = ?", (file_id,))
    row = c.fetchone()
    conn.close()

    if not row:
        raise HTTPException(404, "File not found")

    result = pedro_enrich_file(row["original_path"])
    return result
