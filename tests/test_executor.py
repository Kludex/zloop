from __future__ import annotations

import asyncio
import concurrent.futures
import socket
import time

import pytest

from tests.conftest import run


def test_run_in_executor_default(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> int:
        return await loop.run_in_executor(None, lambda: 6 * 7)

    assert run(loop, main()) == 42


def test_run_in_executor_blocking_work(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> str:
        def work() -> str:
            time.sleep(0.05)
            return "done"

        return await loop.run_in_executor(None, work)

    assert run(loop, main()) == "done"


def test_run_in_executor_custom_executor(loop: asyncio.AbstractEventLoop) -> None:
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)

    async def main() -> int:
        return await loop.run_in_executor(ex, lambda: 99)

    try:
        assert run(loop, main()) == 99
    finally:
        ex.shutdown()


def test_set_default_executor(loop: asyncio.AbstractEventLoop) -> None:
    ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
    loop.set_default_executor(ex)

    async def main() -> int:
        return await loop.run_in_executor(None, lambda: 7)

    try:
        assert run(loop, main()) == 7
    finally:
        ex.shutdown()


def test_shutdown_default_executor(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        # create the default executor by using it
        await loop.run_in_executor(None, lambda: 1)
        await loop.shutdown_default_executor()

    run(loop, main())


def test_shutdown_default_executor_when_none(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        await loop.shutdown_default_executor()

    run(loop, main())


def test_shutdown_default_executor_error_propagates(loop: asyncio.AbstractEventLoop) -> None:
    class BadExecutor(concurrent.futures.ThreadPoolExecutor):
        def shutdown(self, *args: object, **kwargs: object) -> None:
            raise RuntimeError("shutdown boom")

    loop.set_default_executor(BadExecutor(max_workers=1))

    async def main() -> None:
        with pytest.raises(RuntimeError, match="shutdown boom"):
            await loop.shutdown_default_executor()

    run(loop, main())


def test_getaddrinfo(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> list:
        return await loop.getaddrinfo("127.0.0.1", 80, type=socket.SOCK_STREAM)

    infos = run(loop, main())
    assert any(info[4][0] == "127.0.0.1" for info in infos)


def test_getnameinfo(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> tuple:
        return await loop.getnameinfo(("127.0.0.1", 80), socket.NI_NUMERICHOST | socket.NI_NUMERICSERV)

    host, service = run(loop, main())
    assert host == "127.0.0.1"


def test_shutdown_asyncgens(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        await loop.shutdown_asyncgens()

    run(loop, main())
