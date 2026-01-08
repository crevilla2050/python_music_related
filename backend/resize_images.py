#!/usr/bin/env python3
"""
backend/resize_images.py

Album-art normalization utility used by Pedro Organiza.

This module provides a single, well-tested helper `normalize_image`
that converts common image formats to deterministic, square JPEGs with
EXIF removed and controlled dimensions. The function is intentionally
conservative (no upscaling by default) and supports a `return_bytes`
mode for callers that want an in-memory JPEG rather than writing to
disk.

Normalization goals:
- Produce square images via center crop
- Remove EXIF metadata and fix orientation via EXIF transpose
- Resize down to `max_size` while optionally avoiding upscaling
- Output JPEG with predictable quality and progressive encoding
"""

import sys
import io
from pathlib import Path
from PIL import Image, ImageOps

from backend.i18n.messages import msg

# ---------------- CONFIG ----------------

MAX_SIZE = 1024
MIN_SIZE = 300
JPEG_QUALITY = 90

SUPPORTED_EXTS = {".jpg", ".jpeg", ".png", ".webp"}

# ---------------- CORE LOGIC ----------------

def normalize_image(
    src_path: Path,
    dst_path: Path | None = None,
    *,
    max_size: int = MAX_SIZE,
    min_size: int = MIN_SIZE,
    jpeg_quality: int = JPEG_QUALITY,
    allow_upscale: bool = False,
    return_bytes: bool = False,
) -> bool | bytes:
    """
    Normalize a single image.

    Modes:
    - return_bytes = False (default):
        Writes normalized image to dst_path or src_path.
        Returns True if written, False if skipped.
    - return_bytes = True:
        Does NOT write to disk.
        Returns JPEG bytes if successful, None if skipped.

    Rules:
    - EXIF orientation fixed
    - EXIF stripped
    - Center-crop to square
    - Resize down to max_size
    - No upscaling by default
    """

    try:
        with Image.open(src_path) as img:
            # Apply EXIF transpose to fix orientation and remove EXIF
            img = ImageOps.exif_transpose(img)

            # Work in RGB to have deterministic bytes regardless of input
            img = img.convert("RGB")
            w, h = img.size

            # Reject images that are too small to be useful
            if w < min_size or h < min_size:
                return None if return_bytes else False

            # Center-crop to a square based on the smaller side
            side = min(w, h)
            left = (w - side) // 2
            top = (h - side) // 2
            img = img.crop((left, top, left + side, top + side))

            # Resize behaviour:
            # - If the image is larger than `max_size`, scale down.
            # - If the image is smaller and upscaling is disallowed, keep
            #   the current size (caller may treat this as 'skip').
            if side > max_size:
                img = img.resize((max_size, max_size), Image.LANCZOS)
            elif side < max_size and not allow_upscale:
                # Intentionally do nothing (we will return 'skipped')
                pass
            else:
                img = img.resize((max_size, max_size), Image.LANCZOS)

            # Return bytes mode: useful for embedding or in-memory flows
            if return_bytes:
                buf = io.BytesIO()
                img.save(
                    buf,
                    format="JPEG",
                    quality=jpeg_quality,
                    optimize=True,
                    progressive=True,
                )
                return buf.getvalue()

            # File mode: write to `dst_path` if provided, otherwise overwrite
            # the source image with the normalized JPEG.
            out_path = dst_path or src_path

            img.save(
                out_path,
                "JPEG",
                quality=jpeg_quality,
                optimize=True,
                progressive=True,
            )

            return True

    except Exception:
        # Fail permissively: callers usually treat False/None as 'skipped'
        return None if return_bytes else False


# ---------------- CLI ----------------

def main():
    if len(sys.argv) < 2:
        print(msg("RESIZE_USAGE"))
        sys.exit(1)

    target = Path(sys.argv[1])

    if target.is_file():
        ok = normalize_image(target)
        print(msg("RESIZE_OK") if ok else msg("RESIZE_SKIPPED"))
        return

    if not target.is_dir():
        print(msg("RESIZE_INVALID_PATH"))
        sys.exit(1)

    processed = 0
    skipped = 0

    for p in target.rglob("*"):
        if p.suffix.lower() not in SUPPORTED_EXTS:
            continue

        if normalize_image(p):
            processed += 1
        else:
            skipped += 1

    print(
        msg("RESIZE_SUMMARY").format(
            processed=processed,
            skipped=skipped
        )
    )


if __name__ == "__main__":
    main()

# ---------------- END OF FILE ----------------
