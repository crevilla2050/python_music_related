import os
import shutil
import unicodedata
from rapidfuzz import fuzz

def normalize_artist_name(name):
    nfkd_form = unicodedata.normalize('NFD', name)
    without_accents = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    return without_accents.lower().strip()

def find_groups(artist_dirs, threshold=85):
    normalized = [(artist, normalize_artist_name(artist)) for artist in artist_dirs]
    groups = []

    for artist, norm_name in normalized:
        placed = False
        for group in groups:
            # Compare with first normalized name of group
            rep_norm = normalize_artist_name(group[0])
            score = fuzz.ratio(norm_name, rep_norm)
            if score >= threshold:
                group.append(artist)
                placed = True
                break
        if not placed:
            groups.append([artist])

    return groups

def merge_artist_folders(base_path, threshold=85):
    artist_dirs = [d for d in os.listdir(base_path) if os.path.isdir(os.path.join(base_path, d))]

    groups = find_groups(artist_dirs, threshold)

    for group in groups:
        if len(group) > 1:
            canonical_folder = group[0]
            canonical_path = os.path.join(base_path, canonical_folder)

            print(f"\n[+] Merging folders: {group} → '{canonical_folder}'")

            for dup_folder in group[1:]:
                dup_path = os.path.join(base_path, dup_folder)
                # Move items safely
                for item in os.listdir(dup_path):
                    src = os.path.join(dup_path, item)
                    dst = os.path.join(canonical_path, item)

                    if os.path.exists(dst):
                        base, ext = os.path.splitext(item)
                        counter = 1
                        while True:
                            new_name = f"{base}_{counter}{ext}"
                            new_dst = os.path.join(canonical_path, new_name)
                            if not os.path.exists(new_dst):
                                dst = new_dst
                                break
                            counter += 1

                    shutil.move(src, dst)
                    print(f"  Moved {src} → {dst}")

                os.rmdir(dup_path)
                print(f"  Removed empty folder {dup_path}")

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python merge_artist_folders_fuzzy.py <Organizadas_path> [similarity_threshold]")
        sys.exit(1)

    base_dir = sys.argv[1]
    threshold = int(sys.argv[2]) if len(sys.argv) > 2 else 85

    merge_artist_folders(base_dir, threshold)
    print("\n[✔] Artist folders merged using fuzzy matching.")

