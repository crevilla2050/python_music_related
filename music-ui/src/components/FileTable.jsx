import { useState, useRef, useMemo } from "react";

/* ---------- helpers ---------- */

/**
 * Checks if a value is considered "unknown"
 * @param {string} value - The value to check
 * @returns {boolean} - True if the value is null, undefined, empty, or starts with "unknown"
 */
function isUnknown(value) {
  // First check if the value is falsy (null, undefined, empty string, etc.)
  if (!value) return true;
  // Check if the lowercase version of the value starts with "unknown"
  return value.toLowerCase().startsWith("unknown");
}

const ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ".split("");

/* ---------- component ---------- */

function FileTable({ files }) {
  const audioRef = useRef(null);
  const [playingId, setPlayingId] = useState(null);

  /* ---------- rows ---------- */

  const [rows, setRows] = useState(
    files.map(f => ({
      ...f,
      _selected: false,
      _edit: {
        artist: f.artist || "",
        album: f.album || "",
        title: f.title || "",
        proposed_path: f.recommended_path || "",
        pathManuallyEdited: false
      },
      _pedro: {
        loading: false,
        result: null,
        error: null
      }
    }))
  );

  /* ---------- UI state ---------- */

  const [showFilters, setShowFilters] = useState(true);
  const [showBulk, setShowBulk] = useState(true);

  const [bulkEdit, setBulkEdit] = useState({
    artist: "",
    album: "",
    title: "",
    proposed_path: ""
  });

  const [filters, setFilters] = useState({
    artist: "",
    album: "",
    title: "",
    onlyUnknownArtist: false,
    onlyUnknownAlbum: false,
    onlyUnknownTitle: false
  });

  const [alphaLetter, setAlphaLetter] = useState("");
  const [alphaField, setAlphaField] = useState("artist");

  /* ---------- audio ---------- */

  const play = (id) => {
    if (!audioRef.current) return;

    if (playingId === id) {
      audioRef.current.pause();
      setPlayingId(null);
    } else {
      audioRef.current.src = `http://127.0.0.1:8000/audio/${id}`;
      audioRef.current.play();
      setPlayingId(id);
    }
  };

  const jump = (sec) => {
    if (audioRef.current) {
      audioRef.current.currentTime += sec;
    }
  };

  /* ---------- helpers ---------- */

  const updateRowField = (id, field, value, manualPath = false) => {
    setRows(prev =>
      prev.map(r => {
        if (r.id !== id) return r;

        const nextEdit = { ...r._edit, [field]: value };
        if (manualPath) nextEdit.pathManuallyEdited = true;

        return { ...r, _selected: true, _edit: nextEdit };
      })
    );
  };

  const toggleSelect = (id) => {
    setRows(prev =>
      prev.map(r =>
        r.id === id ? { ...r, _selected: !r._selected } : r
      )
    );
  };

  /* ---------- bulk ---------- */

  const applyBulkEdit = () => {
    setRows(prev =>
      prev.map(r => {
        if (!r._selected) return r;

        const updated = { ...r._edit };

        if (bulkEdit.artist) updated.artist = bulkEdit.artist;
        if (bulkEdit.album) updated.album = bulkEdit.album;
        if (bulkEdit.title) updated.title = bulkEdit.title;

        if (bulkEdit.proposed_path) {
          updated.proposed_path = bulkEdit.proposed_path;
          updated.pathManuallyEdited = true;
        }

        return { ...r, _edit: updated };
      })
    );

    setBulkEdit({ artist: "", album: "", title: "", proposed_path: "" });
  };

  /* ---------- Pedro ---------- */

  const askPedro = async (row) => {
    setRows(prev =>
      prev.map(r =>
        r.id === row.id
          ? { ...r, _pedro: { loading: true, result: null, error: null } }
          : r
      )
    );

    try {
      const res = await fetch(`http://127.0.0.1:8000/pedro/enrich/${row.id}`, {
        method: "POST"
      });
      const data = await res.json();

      setRows(prev =>
        prev.map(r =>
          r.id === row.id
            ? { ...r, _pedro: { loading: false, result: data, error: null } }
            : r
        )
      );
    } catch {
      setRows(prev =>
        prev.map(r =>
          r.id === row.id
            ? { ...r, _pedro: { loading: false, result: null, error: "Pedro failed" } }
            : r
        )
      );
    }
  };

  const applyPedro = (row) => {
    const tags = row._pedro?.result?.tags;
    if (!tags) return;

    updateRowField(row.id, "artist", tags.artist || "");
    updateRowField(row.id, "album", tags.album || "");
    updateRowField(row.id, "title", tags.title || "");
  };

  /* ---------- filtering ---------- */

  const filteredRows = useMemo(() => {
    return rows.filter(r => {
      if (filters.artist && !r._edit.artist.toLowerCase().includes(filters.artist.toLowerCase())) return false;
      if (filters.album && !r._edit.album.toLowerCase().includes(filters.album.toLowerCase())) return false;
      if (filters.title && !r._edit.title.toLowerCase().includes(filters.title.toLowerCase())) return false;

      if (filters.onlyUnknownArtist && !isUnknown(r.artist)) return false;
      if (filters.onlyUnknownAlbum && !isUnknown(r.album)) return false;
      if (filters.onlyUnknownTitle && !isUnknown(r.title)) return false;

      if (alphaLetter) {
        const v = (r._edit[alphaField] || "").toUpperCase();
        if (!v.startsWith(alphaLetter)) return false;
      }

      return true;
    });
  }, [rows, filters, alphaLetter, alphaField]);

  /* ---------- render ---------- */

  return (
    <>
      <audio ref={audioRef} preload="metadata" />

      {/* ===== ONE sticky container — NO GAPS EVER ===== */}
      <div
        className="sticky-top bg-white border-bottom"
        style={{ zIndex: 1000 }}
      >
        {/* FILTERS */}
        <div className="p-2 border-bottom">
          <label>
            <input type="checkbox" checked={showFilters} onChange={() => setShowFilters(!showFilters)} /> Filters
          </label>

          {showFilters && (
            <>
              <div className="row g-2 mt-2">
                <div className="col"><input className="form-control form-control-sm" placeholder="Artist" value={filters.artist} onChange={e => setFilters({ ...filters, artist: e.target.value })} /></div>
                <div className="col"><input className="form-control form-control-sm" placeholder="Album" value={filters.album} onChange={e => setFilters({ ...filters, album: e.target.value })} /></div>
                <div className="col"><input className="form-control form-control-sm" placeholder="Title" value={filters.title} onChange={e => setFilters({ ...filters, title: e.target.value })} /></div>
              </div>

              <div className="mt-2 d-flex gap-3">
                <label><input type="checkbox" checked={filters.onlyUnknownArtist} onChange={e => setFilters({ ...filters, onlyUnknownArtist: e.target.checked })} /> Unknown artist</label>
                <label><input type="checkbox" checked={filters.onlyUnknownAlbum} onChange={e => setFilters({ ...filters, onlyUnknownAlbum: e.target.checked })} /> Unknown album</label>
                <label><input type="checkbox" checked={filters.onlyUnknownTitle} onChange={e => setFilters({ ...filters, onlyUnknownTitle: e.target.checked })} /> Unknown title</label>
              </div>

              <div className="mt-2 d-flex align-items-center gap-2">
                <select className="form-select form-select-sm w-auto" value={alphaField} onChange={e => setAlphaField(e.target.value)}>
                  <option value="artist">Artist</option>
                  <option value="album">Album</option>
                </select>

                {ALPHABET.map(l => (
                  <button key={l} className={`btn btn-sm ${alphaLetter === l ? "btn-dark" : "btn-outline-dark"}`} onClick={() => setAlphaLetter(l)}>
                    {l}
                  </button>
                ))}
                <button className="btn btn-sm btn-outline-secondary" onClick={() => setAlphaLetter("")}>All</button>
              </div>
            </>
          )}
        </div>

        {/* BULK */}
        <div className="p-2 border-bottom bg-light">
          <label>
            <input type="checkbox" checked={showBulk} onChange={() => setShowBulk(!showBulk)} /> Bulk edit (selected)
          </label>

          {showBulk && (
            <>
              <div className="row g-2 mt-2">
                <div className="col"><input className="form-control form-control-sm" placeholder="Artist" value={bulkEdit.artist} onChange={e => setBulkEdit({ ...bulkEdit, artist: e.target.value })} /></div>
                <div className="col"><input className="form-control form-control-sm" placeholder="Album" value={bulkEdit.album} onChange={e => setBulkEdit({ ...bulkEdit, album: e.target.value })} /></div>
                <div className="col"><input className="form-control form-control-sm" placeholder="Title" value={bulkEdit.title} onChange={e => setBulkEdit({ ...bulkEdit, title: e.target.value })} /></div>
              </div>

              <input className="form-control form-control-sm mt-2" placeholder="Proposed path" value={bulkEdit.proposed_path} onChange={e => setBulkEdit({ ...bulkEdit, proposed_path: e.target.value })} />

              <button className="btn btn-sm btn-success mt-2" onClick={applyBulkEdit}>Apply to selected</button>
            </>
          )}
        </div>

        {/* TABLE HEADER */}
        <table className="table table-sm mb-0" style={{ tableLayout: "fixed" }}>
          <thead style={{ background: "#e9ecef", color: "#000" }}>
            <tr>
              <th style={{ width: 40 }}></th>
              <th style={{ width: 60 }}>ID</th>
              <th>Artist</th>
              <th>Title</th>
              <th>Album</th>
              <th style={{ width: 180 }}>Preview</th>
            </tr>
          </thead>
        </table>
      </div>

      {/* ================= TABLE BODY ================= */}
      <table className="table table-sm table-striped align-middle" style={{ tableLayout: "fixed" }}>
        <tbody>
          {filteredRows.map(row => (
            <>
              <tr key={row.id}>
                <td style={{ width: 40 }}>
                  <input type="checkbox" checked={row._selected} onChange={() => toggleSelect(row.id)} />
                </td>
                <td style={{ width: 60 }}>{row.id}</td>
                <td><input className="form-control form-control-sm" disabled={!row._selected} value={row._edit.artist} onChange={e => updateRowField(row.id, "artist", e.target.value)} /></td>
                <td><input className="form-control form-control-sm" disabled={!row._selected} value={row._edit.title} onChange={e => updateRowField(row.id, "title", e.target.value)} /></td>
                <td><input className="form-control form-control-sm" disabled={!row._selected} value={row._edit.album} onChange={e => updateRowField(row.id, "album", e.target.value)} /></td>
                <td style={{ width: 180 }}>
                  <button className="btn btn-sm btn-primary me-1" onClick={() => play(row.id)}>{playingId === row.id ? "⏸" : "▶"}</button>
                  <button className="btn btn-sm btn-outline-secondary me-1" onClick={() => jump(-10)}>−10s</button>
                  <button className="btn btn-sm btn-outline-secondary" onClick={() => jump(10)}>+10s</button>
                </td>
              </tr>

              <tr className="table-light">
                <td colSpan={6}>
                  <input className="form-control form-control-sm" value={row._edit.proposed_path} onChange={e => updateRowField(row.id, "proposed_path", e.target.value, true)} />
                </td>
              </tr>

              <tr className="table-info">
                <td colSpan={6} className="d-flex align-items-center gap-3">
                  <strong>Pedro:</strong>
                  {row._pedro.loading && <span>Thinking…</span>}
                  {row._pedro.error && <span className="text-danger">{row._pedro.error}</span>}
                  {row._pedro.result && <span className="text-success">Match ({Math.round((row._pedro.result.confidence || 0) * 100)}%)</span>}
                  <button className="btn btn-sm btn-outline-primary" onClick={() => askPedro(row)}>Ask Pedro</button>
                  <button className="btn btn-sm btn-success" disabled={!row._pedro.result} onClick={() => applyPedro(row)}>Apply Pedro</button>
                </td>
              </tr>
            </>
          ))}
        </tbody>
      </table>
    </>
  );
}

export default FileTable;
