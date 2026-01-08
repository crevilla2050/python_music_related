import os
import sys

# Allowed "image-only garbage" extensions
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".gif", ".bmp", ".tiff"}

# Maximum size (bytes) for small trash images
MAX_IMAGE_SIZE = 100 * 1024   # 100 KB

def is_small_image(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    if ext not in IMAGE_EXTS:
        return False
    try:
        return os.path.getsize(file_path) <= MAX_IMAGE_SIZE
    except:
        return False


def directory_is_deletable(path):
    """
    Conditions for deletion:
    - Directory is empty.
    - OR contains only small images (<100 KB).
    - OR contains only other directories that will also be deleted (handled by bottom-up walk).
    """
    try:
        entries = [os.path.join(path, f) for f in os.listdir(path)]
    except Exception:
        return False

    if not entries:
        return True  # Empty directory

    for item in entries:
        if os.path.isdir(item):
            # Subdirectories will be evaluated in the walk loop after we evaluate them individually
            continue

        # File: Accept only small images
        if not is_small_image(item):
            return False

    return True  # Only small images (all valid trash)


def remove_directory_with_small_images(path):
    """
    Deletes small images inside the directory, then removes the directory.
    """
    try:
        for f in os.listdir(path):
            fp = os.path.join(path, f)

            if os.path.isfile(fp) and is_small_image(fp):
                try:
                    os.remove(fp)
                    print(f"[🗑️] Deleted small image ({os.path.getsize(fp)} bytes): {fp}")
                except Exception as e:
                    print(f"[!] Error deleting file {fp}: {e}")

        # Now directory should be empty or only subfolders that will be removed later
        if not os.listdir(path):
            os.rmdir(path)
            print(f"[🗑️] Removed empty/trash-only directory: {path}")
            return True

    except Exception as e:
        print(f"[!] Failed processing directory {path}: {e}")

    return False


def clean_directories(path):
    removed_count = 0

    for root, dirs, files in os.walk(path, topdown=False):
        for d in dirs:
            full_path = os.path.join(root, d)

            if directory_is_deletable(full_path):
                if remove_directory_with_small_images(full_path):
                    removed_count += 1

    return removed_count


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python clean_empty_dirs.py <target_directory>")
        sys.exit(1)

    target = sys.argv[1]

    if not os.path.isdir(target):
        print(f"[✗] Not a valid directory: {target}")
        sys.exit(1)

    print(f"[INFO] Cleaning empty or trash-only directories in: {target}")
    count = clean_directories(target)
    print(f"[✓] Done. Removed {count} director{'y' if count == 1 else 'ies'}.")
