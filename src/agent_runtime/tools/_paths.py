"""pi path normalization and local read fallbacks. cwd is not a security boundary."""

import os
import re
import unicodedata
from pathlib import Path
from urllib.parse import unquote, urlsplit


def resolve_path(path: str, cwd: str) -> str:
    path = re.sub(r"[\u00a0\u2000-\u200a\u202f\u205f\u3000]", " ", path)
    path = path.removeprefix("@")
    if path == "~" or path.startswith("~/"):
        path = os.path.expanduser(path)
    if path.startswith("file://"):
        url = urlsplit(path)
        if url.netloc not in ("", "localhost"):
            raise ValueError("File URLs must refer to the local host")
        path = unquote(url.path)
    return os.path.abspath(os.path.join(cwd, path))


def resolve_read_path(path: str, cwd: str) -> str:
    resolved = resolve_path(path, cwd)
    nfd = unicodedata.normalize("NFD", resolved)
    candidates = (
        resolved,
        re.sub(r" (AM|PM)\.", "\u202f\\1.", resolved, flags=re.IGNORECASE),
        nfd,
        resolved.replace("'", "\u2019"),
        nfd.replace("'", "\u2019"),
    )
    return next((p for p in candidates if Path(p).exists()), resolved)
