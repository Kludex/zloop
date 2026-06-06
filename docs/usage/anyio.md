---
icon: lucide/git-merge
---

# With AnyIO

[AnyIO](https://anyio.readthedocs.io) is the structured-concurrency layer that
sits on top of asyncio (and Trio). It's what Starlette, FastAPI, and HTTPX use
internally - so making AnyIO run on zloop means a *lot* of the ecosystem runs on
zloop.

The good news: AnyIO's asyncio backend accepts a **loop factory**, so this is a
clean one-liner.

## `anyio.run()`

```python title="main.py" hl_lines="17"
import anyio

import zloop


async def main():
    async with anyio.create_task_group() as tg:
        tg.start_soon(anyio.sleep, 0.1)
        tg.start_soon(anyio.sleep, 0.2)
    return "structured concurrency on a Zig loop ✨"


print(
    anyio.run(
        main,
        backend="asyncio",
        backend_options={"loop_factory": zloop.new_event_loop},  # (1)!
    )
)
```

1.  AnyIO's asyncio backend forwards `loop_factory` straight to an
    `asyncio.Runner`. Same mechanism as everywhere else.

## In tests (`pytest` + `anyio`)

If you test with the `anyio` pytest plugin, you select the backend with the
`anyio_backend` fixture. Return a tuple to pass options - including the loop
factory:

```python title="conftest.py"
import pytest

import zloop


@pytest.fixture
def anyio_backend():
    return ("asyncio", {"loop_factory": zloop.new_event_loop})  # (1)!
```

1.  Now every `@pytest.mark.anyio` test in your suite runs on zloop. This is
    exactly how we run uvicorn's own test suite against zloop.

```python title="test_something.py"
import asyncio

import pytest


@pytest.mark.anyio
async def test_runs_on_zloop():
    assert type(asyncio.get_running_loop()).__module__.startswith("zloop")
```

!!! tip
    This fixture trick is the easiest way to run an *existing* asyncio/AnyIO test
    suite on zloop and see what happens. Suites that stick to TCP/TLS sockets,
    streams, tasks, and timers should pass unchanged; if a suite reaches for
    UDP, subprocesses, pipes, or the `sock_*` helpers it'll hit zloop's
    [unimplemented APIs](../reference/compatibility.md). 🙂
