import os
import sys
import json
from datetime import datetime

from mutagen import File as MutagenFile

def get_media_metadata(filepath):
    try:
        audio = MutagenFile(filepath, easy=True)
        if audio is None:
            return {}
        # Common tags
        tags = {}
        for tag in ["artist", "album", "title", "tracknumber", "date", "genre"]:
            if tag in audio:
                tags[tag] = audio[tag][0]
        # Duration in seconds
        duration = getattr(audio.info, "length", None)
        if duration is not None:
            tags["duration_sec"] = round(duration, 2)
        # Bitrate (kbps)
        bitrate = getattr(audio.info, "bitrate", None)
        if bitrate is not None:
            tags["bitrate_kbps"] = int(bitrate / 1000)
        return tags
    except Exception:
        return {}

def snapshot_directory(root_dir, enrich_metadata=False):
    """
    Recursively scan directory and build nested dictionary with folder and file info.
    If enrich_metadata=True, gather media metadata for files.
    """
    tree = {}

    for dirpath, dirnames, filenames in os.walk(root_dir):
        rel_dir = os.path.relpath(dirpath, root_dir)
        # Normalize rel_dir for root folder
        rel_dir = "" if rel_dir == "." else rel_dir

        # Navigate/create nested dict structure for current directory
        node = tree
        if rel_dir:
            for part in rel_dir.split(os.sep):
                node = node.setdefault(part, {})

        for fname in sorted(filenames):
            full_path = os.path.join(dirpath, fname)
            try:
                stat = os.stat(full_path)
                file_info = {
                    "size_bytes": stat.st_size,
                    "modified_time": datetime.fromtimestamp(stat.st_mtime).isoformat()
                }
                if enrich_metadata:
                    file_info["metadata"] = get_media_metadata(full_path)
                node[fname] = file_info
            except Exception as e:
                print(f"[!] Failed to stat '{full_path}': {e}")

    return tree

def write_tree_text(node, depth=0, file_handle=None):
    """
    Write nested dict tree as a clean folder tree, showing folders and filenames only,
    no metadata or sizes.
    """
    for key in sorted(node.keys()):
        if isinstance(node[key], dict) and any(isinstance(v, dict) for v in node[key].values()):
            # Folder detected if contains dict children that are dicts (files have dict with keys)
            file_handle.write(f"{'  ' * depth}📁 {key}/\n")
            write_tree_text(node[key], depth + 1, file_handle)
        else:
            # File detected
            file_handle.write(f"{'  ' * depth}🎵 {key}\n")

def save_snapshot(tree, output_file, json_mode=True):
    if json_mode:
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(tree, f, indent=2, ensure_ascii=False)
    else:
        with open(output_file, "w", encoding="utf-8") as f:
            write_tree_text(tree, file_handle=f)

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python snapshot_music_library.py <target_directory> <output_file> [--text]")
        sys.exit(1)

    target_dir = sys.argv[1]
    output_path = sys.argv[2]
    output_as_text = "--text" in sys.argv

    if not os.path.isdir(target_dir):
        print(f"[✗] Invalid directory: {target_dir}")
        sys.exit(1)

    print(f"[INFO] Scanning '{target_dir}'...")
    tree = snapshot_directory(target_dir, enrich_metadata=not output_as_text)
    save_snapshot(tree, output_path, json_mode=not output_as_text)
    print(f"[✓] Snapshot saved to '{output_path}'")
