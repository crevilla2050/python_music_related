import os
import sys

def fix_mp3_filenames(root_dir):
    """
    Recursively crawl through root_dir and rename files ending with '_mp3' 
    by replacing '_mp3' with '.mp3' extension.
    """
    for dirpath, dirnames, filenames in os.walk(root_dir):
        for filename in filenames:
            if filename.endswith('_mp3'):
                old_path = os.path.join(dirpath, filename)
                new_filename = filename[:-4] + '.mp3'  # remove '_mp3' and add '.mp3'
                new_path = os.path.join(dirpath, new_filename)
                try:
                    os.rename(old_path, new_path)
                    print(f"Renamed: {old_path} -> {new_path}")
                except Exception as e:
                    print(f"Failed to rename {old_path}: {e}")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        directory = sys.argv[1]
    else:
        directory = os.getcwd()
    print(f"Starting to fix mp3 filenames in directory: {directory}")
    fix_mp3_filenames(directory)
