# genre_service.py

from datetime import datetime, timezone
import unicodedata
import re
from collections import defaultdict

EDITABLE_STATES = {"new", "reviewing"}


# -------------------------------------------------
# Utilities
# -------------------------------------------------

def utcnow():
    return datetime.now(timezone.utc).isoformat()


def normalize_token(s: str) -> str:
    """
    Normalize for comparison only.
    Keeps Unicode display elsewhere.
    """
    if not s:
        return ""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"\s+", " ", s)
    return s.strip()


# -------------------------------------------------
# Read operations
# -------------------------------------------------

def list_genres(conn):
    return conn.execute("""
        SELECT id, name, normalized_name
        FROM genres
        ORDER BY normalized_name
    """).fetchall()


def get_file_genres(conn, file_id):
    return conn.execute("""
        SELECT
            g.id,
            g.name,
            fg.source,
            fg.confidence
        FROM file_genres fg
        JOIN genres g ON g.id = fg.genre_id
        WHERE fg.file_id = ?
        ORDER BY g.normalized_name
    """, (file_id,)).fetchall()


# -------------------------------------------------
# Genre grouping (heuristic, non-destructive)
# -------------------------------------------------

def group_similar_genres(conn):
    """
    Returns a list of groups.
    Each group is a list of genre rows that appear similar.
    """
    rows = list_genres(conn)

    buckets = defaultdict(list)

    for r in rows:
        key = normalize_token(r["normalized_name"])
        # Reduce to root token for loose grouping
        root = key.split(" ")[0]
        buckets[root].append(r)

    # Only return groups with more than one entry
    return [
        group for group in buckets.values()
        if len(group) > 1
    ]


# -------------------------------------------------
# Lifecycle enforcement
# -------------------------------------------------

def assert_files_editable(conn, file_ids):
    rows = conn.execute("""
        SELECT DISTINCT lifecycle_state
        FROM files
        WHERE id IN ({})
    """.format(",".join("?" * len(file_ids))), tuple(file_ids)).fetchall()

    blocked = {r["lifecycle_state"] for r in rows} - EDITABLE_STATES
    if blocked:
        raise RuntimeError(
            f"Files not editable due to lifecycle_state: {blocked}"
        )


# -------------------------------------------------
# Filtering
# -------------------------------------------------

def filter_files_by_genres(
    conn,
    genre_ids,
    mode="any",
    only_states=None,
    exclude_states=None,
):
    genre_ids = list(genre_ids)
    if not genre_ids:
        return []

    state_where = []
    params = []

    if only_states:
        state_where.append(
            f"f.lifecycle_state IN ({','.join('?' * len(only_states))})"
        )
        params.extend(only_states)

    if exclude_states:
        state_where.append(
            f"f.lifecycle_state NOT IN ({','.join('?' * len(exclude_states))})"
        )
        params.extend(exclude_states)

    state_sql = " AND ".join(state_where)
    if state_sql:
        state_sql = " AND " + state_sql

    if mode == "any":
        sql = f"""
            SELECT DISTINCT f.id
            FROM files f
            JOIN file_genres fg ON fg.file_id = f.id
            WHERE fg.genre_id IN ({','.join('?' * len(genre_ids))})
            {state_sql}
        """
        params = genre_ids + params

    elif mode == "all":
        sql = f"""
            SELECT f.id
            FROM files f
            JOIN file_genres fg ON fg.file_id = f.id
            WHERE fg.genre_id IN ({','.join('?' * len(genre_ids))})
            {state_sql}
            GROUP BY f.id
            HAVING COUNT(DISTINCT fg.genre_id) = ?
        """
        params = genre_ids + params + [len(genre_ids)]

    elif mode == "exclude":
        sql = f"""
            SELECT f.id
            FROM files f
            WHERE f.id NOT IN (
                SELECT file_id
                FROM file_genres
                WHERE genre_id IN ({','.join('?' * len(genre_ids))})
            )
            {state_sql}
        """
        params = genre_ids + params

    else:
        raise ValueError("mode must be one of: any, all, exclude")

    return [r["id"] for r in conn.execute(sql, params).fetchall()]


# -------------------------------------------------
# Write operations
# -------------------------------------------------

def add_genres_to_files(conn, file_ids, genre_ids):
    assert_files_editable(conn, file_ids)

    now = utcnow()
    for fid in file_ids:
        for gid in genre_ids:
            conn.execute("""
                INSERT OR IGNORE INTO file_genres
                (file_id, genre_id, source, confidence, created_at)
                VALUES (?, ?, 'user', 1.0, ?)
            """, (fid, gid, now))

    _touch_files(conn, file_ids)


def remove_genres_from_files(conn, file_ids, genre_ids):
    assert_files_editable(conn, file_ids)

    for fid in file_ids:
        for gid in genre_ids:
            conn.execute("""
                DELETE FROM file_genres
                WHERE file_id=? AND genre_id=?
            """, (fid, gid))

    _touch_files(conn, file_ids)


def canonize_genre_group(
    conn,
    source_genre_ids,
    canonical_name: str,
):
    """
    Remap all file_genres from source_genre_ids
    to a single canonical genre.
    """

    canonical_norm = normalize_token(canonical_name)
    now = utcnow()

    # Create or reuse canonical genre
    row = conn.execute("""
        SELECT id FROM genres
        WHERE normalized_name=?
    """, (canonical_norm,)).fetchone()

    if row:
        canonical_id = row["id"]
    else:
        canonical_id = conn.execute("""
            INSERT INTO genres
            (name, normalized_name, source, created_at)
            VALUES (?, ?, 'user', ?)
        """, (canonical_name, canonical_norm, now)).lastrowid

    # Find affected files
    rows = conn.execute(f"""
        SELECT DISTINCT file_id
        FROM file_genres
        WHERE genre_id IN ({','.join('?' * len(source_genre_ids))})
    """, tuple(source_genre_ids)).fetchall()

    file_ids = {r["file_id"] for r in rows}
    if not file_ids:
        return canonical_id

    assert_files_editable(conn, file_ids)

    # Remove old relations
    conn.execute(f"""
        DELETE FROM file_genres
        WHERE genre_id IN ({','.join('?' * len(source_genre_ids))})
    """, tuple(source_genre_ids))

    # Insert canonical relations
    for fid in file_ids:
        conn.execute("""
            INSERT OR IGNORE INTO file_genres
            (file_id, genre_id, source, confidence, created_at)
            VALUES (?, ?, 'user', 1.0, ?)
        """, (fid, canonical_id, now))

    _touch_files(conn, file_ids)

    return canonical_id


def _touch_files(conn, file_ids):
    conn.execute("""
        UPDATE files
        SET lifecycle_state='reviewing',
            last_update=?
        WHERE id IN ({})
    """.format(",".join("?" * len(file_ids))),
    (utcnow(), *file_ids))
