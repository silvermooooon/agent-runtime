"""pi image normalization/resizing strategy, using Pillow instead of Photon."""

import base64
from dataclasses import dataclass
from io import BytesIO

from PIL import Image, ImageOps


@dataclass(frozen=True)
class ImageResizeOptions:
    max_width: int = 2000
    max_height: int = 2000
    max_bytes: int = 4718592  # 4.5 MiB of base64 payload, as in pi.
    jpeg_quality: int = 80

    def __post_init__(self):
        if min(self.max_width, self.max_height, self.max_bytes) <= 0:
            raise ValueError("Image dimensions and byte limit must be positive")
        if not 1 <= self.jpeg_quality <= 100:
            raise ValueError("jpeg_quality must be between 1 and 100")


def detect_image(data):
    if data.startswith(b"\xff\xd8\xff") and data[3:4] != b"\xf7":
        return "image/jpeg"
    if data.startswith(b"\x89PNG\r\n\x1a\n") and data[8:16] == b"\0\0\0\rIHDR":
        offset = 8
        while offset + 8 <= len(data):
            length = int.from_bytes(data[offset : offset + 4], "big")
            kind = data[offset + 4 : offset + 8]
            if kind == b"acTL":
                return None
            if kind == b"IDAT":
                break
            offset += length + 12
        return "image/png"
    if data.startswith((b"GIF87a", b"GIF89a")):
        return "image/gif"
    if data.startswith(b"RIFF") and data[8:12] == b"WEBP":
        return "image/webp"
    if data.startswith(b"BM") and len(data) >= 26:
        return "image/bmp"
    return None


def process_image(data, mime, *, auto_resize=True, options=None):
    options = options or ImageResizeOptions()
    hints = []
    try:
        with Image.open(BytesIO(data)) as source:
            image = ImageOps.exif_transpose(source)
            width, height = image.size
            if mime == "image/bmp":
                buffer = BytesIO()
                image.save(buffer, format="PNG")
                data, mime = buffer.getvalue(), "image/png"
                hints.append("[Image converted from image/bmp to image/png.]")
            if not auto_resize or (
                width <= options.max_width
                and height <= options.max_height
                and ((len(data) + 2) // 3) * 4 < options.max_bytes
            ):
                return data, mime, hints
            scale = min(1, options.max_width / width, options.max_height / height)
            w, h = max(1, int(width * scale + 0.5)), max(1, int(height * scale + 0.5))
            qualities = list(dict.fromkeys([options.jpeg_quality, 85, 70, 55, 40]))
            while True:
                resized = image.resize((w, h), Image.Resampling.LANCZOS)
                for fmt, quality in [("PNG", None), *[("JPEG", q) for q in qualities]]:
                    buffer = BytesIO()
                    candidate = resized if fmt == "PNG" else resized.convert("RGB")
                    candidate.save(buffer, format=fmt, **({"quality": quality} if quality else {}))
                    encoded = buffer.getvalue()
                    if len(base64.b64encode(encoded)) < options.max_bytes:
                        hints.append(
                            f"[Image: original {width}x{height}, displayed at {w}x{h}. "
                            f"Multiply coordinates by {width / w:.2f} to map to original image.]"
                        )
                        return encoded, "image/png" if fmt == "PNG" else "image/jpeg", hints
                if w == h == 1:
                    return None
                w, h = max(1, int(w * 0.75)), max(1, int(h * 0.75))
    except (OSError, ValueError, Image.DecompressionBombError):
        return None
