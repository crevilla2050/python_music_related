import os
import sys
import shutil
import re
from collections import defaultdict

MUSIC_EXTENSIONS = {
    '.mp3', '.wav', '.flac', '.aac', '.ogg', '.m4a', '.wma', '.aiff', '.alac', '.opus'
}

COVER_FILES = [
    'folder.jpg', 'cover.jpg', 'albumart.jpg', 'front.jpg', 'back.jpg', 'disc.jpg', 'AlbumArtSmall.jpg', 'Folder.jpg', 'Thumbs.db'
]

ALBUMART_PATTERN = re.compile(r'^AlbumArt_.*\.jpg$', re.IGNORECASE)

def unique_filename(dest_dir, filename):
    base, ext = os.path.splitext(filename)
    counter = 1
    new_name = filename
    while os.path.exists(os.path.join(dest_dir, new_name)):
        new_name = f"{base}_{counter}{ext}"
        counter += 1
    return new_name

def main():
    if len(sys.argv) != 3:
        print("Usage: python music_mover.py <source_dir> <dest_dir>")
        sys.exit(1)

    source_dir = sys.argv[1]
    dest_dir = sys.argv[2]

    if not os.path.isdir(source_dir):
        print(f"Source directory does not exist or is not a directory: {source_dir}")
        sys.exit(1)
    if not os.path.isdir(dest_dir):
        print(f"Destination directory does not exist or is not a directory: {dest_dir}")
        sys.exit(1)

    other_files = defaultdict(int)
    duplicates = []
    moved_files = 0
    music_dirs = set()
    all_dirs = set()

    log_lines = []
    log_lines.append(f"Starting music file move from '{source_dir}' to '{dest_dir}'\n")

    for root, dirs, files in os.walk(source_dir):
        all_dirs.add(root)
        for file in files:
            file_path = os.path.join(root, file)
            ext = os.path.splitext(file)[1].lower()
            if ext in MUSIC_EXTENSIONS:
                music_dirs.add(root)
                dest_path = os.path.join(dest_dir, file)
                if os.path.exists(dest_path):
                    new_name = unique_filename(dest_dir, file)
                    dest_path = os.path.join(dest_dir, new_name)
                    duplicates.append((file, new_name))
                    log_lines.append(f"Duplicate found: '{file}' renamed to '{new_name}'")
                try:
                    shutil.move(file_path, dest_path)
                    moved_files += 1
                    log_lines.append(f"Moved: '{file_path}' -> '{dest_path}'")
                except Exception as e:
                    log_lines.append(f"Error moving '{file_path}': {e}")
            else:
                other_files[ext] += 1

    # Delete cover files in directories without music
    deleted_covers = 0
    for root in all_dirs:
        if root not in music_dirs:
            for cover in COVER_FILES:
                cover_path = os.path.join(root, cover)
                if os.path.exists(cover_path):
                    try:
                        os.remove(cover_path)
                        deleted_covers += 1
                        log_lines.append(f"Deleted cover file: '{cover_path}'")
                    except Exception as e:
                        log_lines.append(f"Error deleting cover file '{cover_path}': {e}")
            # Check for AlbumArt pattern files
            try:
                for file in os.listdir(root):
                    if ALBUMART_PATTERN.match(file):
                        file_path = os.path.join(root, file)
                        os.remove(file_path)
                        deleted_covers += 1
                        log_lines.append(f"Deleted cover file: '{file_path}'")
            except Exception as e:
                log_lines.append(f"Error listing/deleting files in '{root}': {e}")

    # Delete empty directories
    deleted_dirs = 0
    all_dirs_list = sorted(all_dirs, key=lambda x: x.count(os.sep), reverse=True)
    for root in all_dirs_list:
        try:
            if not os.listdir(root):
                os.rmdir(root)
                deleted_dirs += 1
                log_lines.append(f"Deleted empty directory: '{root}'")
        except Exception as e:
            log_lines.append(f"Error deleting empty directory '{root}': {e}")

    log_lines.append("\nSummary Report:")
    log_lines.append(f"Total music files moved: {moved_files}")
    if duplicates:
        log_lines.append(f"Duplicates renamed: {len(duplicates)}")
        for original, new in duplicates:
            log_lines.append(f"  {original} -> {new}")
    else:
        log_lines.append("No duplicates found.")

    log_lines.append(f"Cover files deleted: {deleted_covers}")
    log_lines.append(f"Empty directories deleted: {deleted_dirs}")

    if other_files:
        log_lines.append("\nOther file types found:")
        for ext, count in sorted(other_files.items()):
            ext_display = ext if ext else "(no extension)"
            log_lines.append(f"  {ext_display}: {count}")
    else:
        log_lines.append("\nNo other file types found.")

    report_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "report.log")
    try:
        with open(report_path, "w") as f:
            f.write("\n".join(log_lines))
        print(f"Report written to {report_path}")
    except Exception as e:
        print(f"Failed to write report log: {e}")

    print("\n".join(log_lines))

if __name__ == "__main__":
    main()
