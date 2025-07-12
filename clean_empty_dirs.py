import os
import sys

def remove_empty_dirs(path):
    removed_count = 0
    for root, dirs, files in os.walk(path, topdown=False):
        for d in dirs:
            full_path = os.path.join(root, d)
            try:
                if not os.listdir(full_path):
                    os.rmdir(full_path)
                    print(f"[🗑️] Removed empty directory: {full_path}")
                    removed_count += 1
            except Exception as e:
                print(f"[!] Failed to remove {full_path}: {e}")
    return removed_count

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python clean_empty_dirs.py <target_directory>")
        sys.exit(1)

    target = sys.argv[1]

    if not os.path.isdir(target):
        print(f"[✗] Not a valid directory: {target}")
        sys.exit(1)

    print(f"[INFO] Cleaning empty directories in: {target}")
    count = remove_empty_dirs(target)
    print(f"[✓] Done. Removed {count} empty director{'y' if count == 1 else 'ies'}.")
