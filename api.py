#!/usr/bin/env python3
"""
api.py

Pedro Organiza — API v1

Purpose:
- Provide a stable, typed API over Pedro's enrichment and planning engines
- Act as a contract boundary between backend logic and UI / clients
- Never perform filesystem mutations directly
- Never leak internal engine structures

This API is intentionally conservative and explicit.
"""

import os
import sqlite3
from datetime import datetime, timezone
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

from new_pedro_tagger import pedro_enrich_file

# ===================== ENV =====================

load_dotenv()

DB_PATH = os.getenv("MUSIC_DB")
if not DB_PATH:
    raise RuntimeError("MUSIC_DB_NOT_SET")

# ===================== APP =====================

app = FastAPI(
    title="Pedro Organiza API",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # tighten later if needed
    allow_methods=["*"],
    allow_headers=["*"],
)

# ===================== HELPERS =====================

def utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ===================== MODELS =====================

class FileSummary(BaseModel):
    id: int
    original_path: str
    artist: Optional[str]
    album_artist: Optional[str]
    album: Optional[str]
    title: Optional[str]
    action: Optional[str]
    status: Optional[str]


class EnrichmentResult(BaseModel):
    success: bool
    confidence: float
    source: str
    notes: Optional[str] = None
    tags: Optional[dict] = None


# ===================== ENDPOINTS =====================

@app.get("/files", response_model=List[FileSummary])
def list_files(
    limit: int = Query(500, ge=1, le=2000),
):
    """
    List files from the staging database.

    This endpoint is intentionally shallow:
    - No joins
    - No derived logic
    - Safe for UI listing and pagination
    """
    conn = get_db()
    rows = conn.execute(
        """
        SELECT
            id,
            original_path,
            artist,
            album_artist,
            album,
            title,
            action,
            status
        FROM files
        ORDER BY id
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    conn.close()

    return [FileSummary(**dict(r)) for r in rows]


@app.post(
    "/files/{file_id}/enrich",
    response_model=EnrichmentResult,
)
def enrich_file(file_id: int):
    """
    Run Pedro enrichment for a single file.

    Notes:
    - Advisory only (no DB writes here)
    - No filesystem mutation
    - Returns a stable enrichment contract
    """
    conn = get_db()
    row = conn.execute(
        "SELECT * FROM files WHERE id = ?",
        (file_id,),
    ).fetchone()
    conn.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="FILE_NOT_FOUND",
        )

    result = pedro_enrich_file(
        source_path=row["original_path"],
        artist_hint=row["artist"],
        title_hint=row["title"],
        album_artist_hint=row["album_artist"],
        is_compilation_hint=row["is_compilation"],
    )

    return EnrichmentResult(
        success=result.get("success", False),
        confidence=result.get("confidence", 0.0),
        source=result.get("source", "unknown"),
        notes=result.get("notes"),
        tags=result.get("tags"),
    )


# ===================== FUTURE EXTENSION POINTS =====================
#
# Planned (NOT implemented here):
#
# - POST /albums/{album_id}/enrich-art
# - GET  /duplicates
# - POST /plan/duplicates
# - POST /execute
# - GET  /actions
#
# The API contract is designed so these can be added without breaking v1.
#
# ================================================================
