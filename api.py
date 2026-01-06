import os
import sqlite3
import mimetypes
from datetime import datetime, timezone
from typing import List

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from dotenv import load_dotenv

from new_pedro_tagger import pedro_enrich_file

load_dotenv()
DB_PATH = os.getenv("MUSIC_DB")

app = FastAPI(title="Music Consolidation API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def utcnow():
    return datetime.now(timezone.utc).isoformat()

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

class FileUpdate(BaseModel):
    id: int
    artist: str | None = None
    album_artist: str | None = None
    album: str | None = None
    title: str | None = None
    is_compilation: int | None = None
    recommended_path: str | None = None

@app.get("/files")
def list_files(limit: int = 500):
    conn = get_db()
    rows = [dict(r) for r in conn.execute("SELECT * FROM files LIMIT ?", (limit,))]
    conn.close()
    return rows

@app.post("/pedro/enrich/{file_id}")
def pedro_enrich(file_id: int):
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM files WHERE id=?", (file_id,)
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(404)

    return pedro_enrich_file(
        source_path=row["original_path"],
        artist=row["artist"],
        album=row["album"],
        album_artist=row["album_artist"],
        is_compilation=row["is_compilation"]
    )
