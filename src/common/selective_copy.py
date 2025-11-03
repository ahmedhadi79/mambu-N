import fnmatch
import os
import shutil

def selective_copy(src_dir, dest_dir, patterns=("*.py", "*.json")):
    """
    Selectively copy files from src_dir to dest_dir based on matching patterns.
    """
    for root, dirs, files in os.walk(src_dir):
        for filename in files:
            for pattern in patterns:
                if fnmatch.fnmatch(filename, pattern):
                    full_src_path = os.path.join(root, filename)
                    rel_path = os.path.relpath(full_src_path, src_dir)
                    full_dest_path = os.path.join(dest_dir, rel_path)
                    os.makedirs(os.path.dirname(full_dest_path), exist_ok=True)
                    shutil.copy2(full_src_path, full_dest_path)