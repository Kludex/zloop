---
icon: lucide/globe
---

# With HTTP clients

zloop is a client-side loop too. Libraries that do their network I/O over
asyncio's TCP/TLS transports - [HTTPX](https://www.python-httpx.org),
[aiohttp](https://docs.aiohttp.org), and most database drivers and message-queue
clients - run on it.

You don't configure the library. You just run your code on a zloop loop, and the
library uses whatever loop is running.

!!! note "Python 3.12+"
    The examples use `asyncio.run(..., loop_factory=...)` (Python 3.12+). On
    3.10 / 3.11, start the loop with `asyncio.Runner` instead - the library code
    is identical. See [First steps](first-steps.md).

## HTTPX

```python title="httpx_example.py"
import asyncio

import httpx

import zloop


async def main():
    async with httpx.AsyncClient() as client:
        r = await client.get("https://example.com")
        print(r.status_code)  #> 200


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

HTTPX's async transport runs on the current event loop - which is zloop. TLS,
connection pooling, timeouts: all handled by zloop's transports under the hood.

## aiohttp

```python title="aiohttp_example.py"
import asyncio

import aiohttp

import zloop


async def main():
    async with aiohttp.ClientSession() as session:
        async with session.get("https://example.com") as resp:
            print(resp.status)  #> 200


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

## The general pattern

This is the recurring theme of these pages, and it's worth saying once clearly:

!!! quote "The whole integration story"
    You pick the loop **once**, at the top, with `loop_factory`. After that, every
    async library in your program runs on it automatically - because they all ask
    asyncio for "the running loop", and that loop is zloop.

So there's no per-library setup. If it's asyncio, it's zloop-compatible.
