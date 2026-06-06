---
icon: lucide/rocket
---

# First steps

zloop gives you one thing: a **loop factory**.

```python
import zloop

zloop.new_event_loop  # a callable that returns a fresh zloop event loop
```

A *loop factory* is just a function that returns a new event loop. asyncio uses
this concept everywhere - and so, to use zloop, you hand this factory to whatever
is in charge of creating the loop.

Let's see the three ways that come up in practice.

## 1. With `asyncio.run()` (Python 3.12+)

This is the modern, recommended way to run asyncio programs:

```python title="main.py" hl_lines="11"
import asyncio

import zloop


async def main():
    await asyncio.sleep(0.1)
    return "done"


print(asyncio.run(main(), loop_factory=zloop.new_event_loop))  # (1)!
```

1.  `loop_factory` was added to `asyncio.run()` in **Python 3.12**. On older
    versions, use the `Runner` approach below.

!!! tip
    `loop_factory` is the cleanest hook there is. No global state, no policies,
    no side effects - just "build the loop with *this*".

## 2. With `asyncio.Runner` (Python 3.11+)

`Runner` is the object `asyncio.run()` uses under the hood. You can use it
directly, which is handy when you want to run several coroutines on the same
loop:

```python
import asyncio

import zloop

with asyncio.Runner(loop_factory=zloop.new_event_loop) as runner:
    runner.run(main())
    runner.run(main())  # same zloop loop, reused
```

## 3. Manually

Sometimes you want the loop object itself. zloop's factory gives you a normal
event loop, so everything you know still applies:

```python
import asyncio

import zloop

loop = zloop.new_event_loop()
try:
    result = loop.run_until_complete(main())
finally:
    loop.close()  # (1)!
```

1.  Always `close()` the loop when you're done. This releases the loop's
    resources (and lets it be garbage-collected cleanly). `asyncio.run()` and
    `Runner` do this for you - which is one more reason to prefer them.

## It's just asyncio

Here's the thing to internalize: once the loop is running, **there is nothing
zloop-specific in your code**. You write asyncio, exactly as you always have.

```python
import asyncio

import zloop


async def main():
    # get the running loop - it's a zloop loop, but you don't care
    loop = asyncio.get_running_loop()

    # schedule a callback
    loop.call_later(0.5, print, "later!")

    # run things concurrently
    results = await asyncio.gather(
        asyncio.sleep(0.1, result="a"),
        asyncio.sleep(0.2, result="b"),
    )
    print(results)


asyncio.run(main(), loop_factory=zloop.new_event_loop)
```

`asyncio.sleep`, `asyncio.gather`, `call_later`, `get_running_loop` - all of it
works, because zloop *is* an asyncio loop. The Zig core is an implementation
detail you opted into with one argument. 🙂

Next, let's plug it into the libraries you actually use.
