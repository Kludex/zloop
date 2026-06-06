---
icon: lucide/server
---

# With uvicorn

[uvicorn](https://www.uvicorn.org) is probably *the* reason you're here. It's the
ASGI server that runs FastAPI, Starlette, and friends - and it lets you choose
the event loop it runs on.

zloop passes uvicorn's **entire test suite**, so it's a true drop-in.

## The CLI

uvicorn's `--loop` flag accepts an import string pointing at a loop factory. Point
it at zloop:

```console
$ uvicorn app:app --loop zloop:new_event_loop
INFO:     Started server process [12345]
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

That's the whole integration. `zloop:new_event_loop` is the same string format
uvicorn uses for `uvloop` and `asyncio` - `module:callable`.

!!! tip "How `--loop` resolves"
    uvicorn keeps built-in names (`auto`, `asyncio`, `uvloop`). For anything
    else, it treats the value as an import string and imports the factory. So
    `zloop:new_event_loop` simply imports `zloop.new_event_loop` and calls it to
    build each worker's loop.

## In code (`Config`)

If you run uvicorn programmatically, set `loop` on the `Config`:

```python title="server.py" hl_lines="11"
import uvicorn


async def app(scope, receive, send):
    assert scope["type"] == "http"
    await send({"type": "http.response.start", "status": 200,
                "headers": [(b"content-type", b"text/plain")]})
    await send({"type": "http.response.body", "body": b"Hello from zloop 👋"})


if __name__ == "__main__":
    uvicorn.run(app, host="127.0.0.1", port=8000, loop="zloop:new_event_loop")  # (1)!
```

1.  Exactly the value you'd pass on the CLI, just as a keyword argument.

## A note on `--loop auto`

uvicorn's `auto` mode prefers `uvloop` if it's installed, otherwise falls back to
`asyncio`. It doesn't know about zloop, so to use zloop you ask for it explicitly
with `--loop zloop:new_event_loop`.

## Does everything work?

Yes - this is the part we're most confident about, because it's continuously
verified:

* HTTP/1.1 (both the `h11` and `httptools` protocols)
* WebSockets (`websockets` and `wsproto`)
* HTTPS / TLS
* Unix domain sockets
* Graceful shutdown and signal handling
* The `--workers` multiprocess model

zloop runs uvicorn's full suite - **1048 tests** - with the same result as the
default asyncio loop. If uvicorn works for you on asyncio, it works on zloop.
