from __future__ import annotations

import asyncio
from collections.abc import Iterator

import pytest

import zloop


@pytest.fixture
def loop() -> Iterator[asyncio.AbstractEventLoop]:
    loop = zloop.new_event_loop()
    try:
        yield loop
    finally:
        if not loop.is_closed():
            loop.close()


def run(loop: asyncio.AbstractEventLoop, coro: object) -> object:
    return loop.run_until_complete(coro)  # type: ignore[arg-type]
