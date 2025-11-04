"""Checksum utilities."""

import hashlib
from pathlib import Path
from typing import Tuple


def md5_file(path: str, chunk_size: int = 1024 * 1024) -> Tuple[bool, str]:
    """
    Compute MD5 for a local file.

    Returns (success, md5_hex or error message).
    """
    try:
        file_path = Path(path)
        if not file_path.exists():
            return False, f"File not found: {path}"

        md5 = hashlib.md5()
        with open(path, "rb") as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                md5.update(chunk)
        return True, md5.hexdigest()
    except Exception as e:
        return False, str(e)


