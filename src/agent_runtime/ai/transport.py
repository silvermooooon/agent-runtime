"""HTTP SSE transport shared by providers and the pi proxy protocol."""

import json

import httpx


async def iter_sse(response):
    data = []
    async for line in response.aiter_lines():
        if line == "":
            if data:
                payload = "\n".join(data)
                data.clear()
                if payload == "[DONE]":
                    yield {"type": "_done"}
                else:
                    yield json.loads(payload)
        elif line.startswith("data:"):
            data.append(line[5:].removeprefix(" "))
    if data:
        payload = "\n".join(data)
        yield {"type": "_done"} if payload == "[DONE]" else json.loads(payload)


async def stream_http(url, payload, headers, *, client=None, timeout=120, on_response=None):
    owned = client is None
    client = client or httpx.AsyncClient()
    try:
        async with client.stream(
            "POST", url, json=payload, headers=headers, timeout=timeout
        ) as response:
            if on_response:
                await on_response(
                    {"status": response.status_code, "headers": dict(response.headers)}
                )
            if not response.is_success:
                body = (await response.aread()).decode(errors="replace")
                raise RuntimeError(f"Provider HTTP {response.status_code}: {body[:2000]}")
            async for event in iter_sse(response):
                yield event
    finally:
        if owned:
            await client.aclose()
