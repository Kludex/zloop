---
icon: lucide/package
---

# Installation

Create a [virtual environment](https://docs.python.org/3/library/venv.html), and
install zloop into it:

```console
$ pip install zloop
```

That's all you need to start. zloop ships as a compiled extension, so there's
nothing to configure and no system libraries to install.

## Requirements

zloop is a CPython extension with a Zig core. It needs:

* **CPython 3.12 or higher**
* **macOS / BSD** (using `kqueue`) or **Linux** (using `epoll`)

!!! info "Why those platforms?"
    The event loop's I/O engine talks directly to the operating system's
    readiness API. On macOS and the BSDs that's `kqueue`; on Linux it's
    `epoll`. Windows isn't supported (yet) - `IOCP` is a different model.

## Verifying the install

Let's make sure it imports and runs:

```python
import asyncio

import zloop

print(zloop.new_event_loop())  # (1)!
asyncio.run(asyncio.sleep(0), loop_factory=zloop.new_event_loop)
print("zloop works ✨")
```

1.  This prints something like `<zloop.Loop object at 0x...>` - a real
    `asyncio.AbstractEventLoop` instance, backed by Zig.

If that runs without errors, you're ready. Head to [First steps](first-steps.md).
