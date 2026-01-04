import { useEffect, useState } from "react";
import FileTable from "./components/FileTable";

function App() {
  const [files, setFiles] = useState([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetch("http://127.0.0.1:8000/files?limit=200")
      .then(res => res.json())
      .then(data => {
        setFiles(data);
        setLoading(false);
      })
      .catch(err => {
        console.error("Failed to fetch files:", err);
        setLoading(false);
      });
  }, []);

  return (
    <div className="container mt-4">
      <h1 className="mb-3">🎵 Music Consolidation Review</h1>

      {loading ? (
        <div className="alert alert-info">Loading files…</div>
      ) : (
        <>
          <p className="text-muted">
            Showing {files.length} files
          </p>
          <FileTable files={files} />
        </>
      )}
    </div>
  );
}

export default App;
