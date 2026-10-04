"""Injectable provider stream function, matching pi's stream-fn.ts."""

_default_stream_fn = None


def set_default_stream_fn(stream_fn):
    global _default_stream_fn
    _default_stream_fn = stream_fn


def get_default_stream_fn():
    if _default_stream_fn is None:
        raise RuntimeError("Pass stream_fn or call set_default_stream_fn() first")
    return _default_stream_fn
