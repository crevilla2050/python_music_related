export default function FileRow({ file }) {
  return (
    <tr>
      <td>{file.id}</td>
      <td>{file.artist || "-"}</td>
      <td>{file.album || "-"}</td>
      <td>{file.title || "-"}</td>

      <td>
        <span className={`badge ${
          file.status === "duplicate" ? "bg-warning" :
          file.status === "unique" ? "bg-success" :
          "bg-secondary"
        }`}>
          {file.status}
        </span>
      </td>

      <td>
        <select
          className="form-select form-select-sm"
          value={file.action}
          disabled
        >
          <option value="move">move</option>
          <option value="skip">skip</option>
          <option value="archive">archive</option>
          <option value="delete">delete</option>
        </select>
      </td>
    </tr>
  );
}
