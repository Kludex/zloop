from __future__ import annotations

import asyncio
import signal
import threading

import pytest

from tests.conftest import run


def test_add_and_fire_signal_handler(loop: asyncio.AbstractEventLoop) -> None:
    fired: asyncio.Future[int] = loop.create_future()

    async def main() -> int:
        loop.add_signal_handler(signal.SIGUSR1, lambda: fired.set_result(1) if not fired.done() else None)
        signal.raise_signal(signal.SIGUSR1)
        result = await asyncio.wait_for(fired, 2.0)
        loop.remove_signal_handler(signal.SIGUSR1)
        return result

    assert run(loop, main()) == 1


def test_add_signal_handler_with_args(loop: asyncio.AbstractEventLoop) -> None:
    got: asyncio.Future[str] = loop.create_future()

    async def main() -> str:
        def handler(value: str) -> None:
            if not got.done():
                got.set_result(value)

        loop.add_signal_handler(signal.SIGUSR2, handler, "payload")
        signal.raise_signal(signal.SIGUSR2)
        result = await asyncio.wait_for(got, 2.0)
        loop.remove_signal_handler(signal.SIGUSR2)
        return result

    assert run(loop, main()) == "payload"


def test_remove_signal_handler_unknown(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bool:
        return loop.remove_signal_handler(signal.SIGUSR1)

    assert run(loop, main()) is False


def test_remove_last_signal_handler_tears_down_pipe(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bool:
        loop.add_signal_handler(signal.SIGUSR1, lambda: None)
        removed = loop.remove_signal_handler(signal.SIGUSR1)
        # adding again after teardown should re-create the pipe and work
        loop.add_signal_handler(signal.SIGUSR1, lambda: None)
        loop.remove_signal_handler(signal.SIGUSR1)
        return removed

    assert run(loop, main()) is True


def test_add_signal_handler_rejects_non_int(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(TypeError):
            loop.add_signal_handler("SIGUSR1", lambda: None)  # type: ignore[arg-type]

    run(loop, main())


def test_two_signal_handlers_share_pipe(loop: asyncio.AbstractEventLoop) -> None:
    got1: asyncio.Future[None] = loop.create_future()
    got2: asyncio.Future[None] = loop.create_future()

    async def main() -> None:
        loop.add_signal_handler(signal.SIGUSR1, lambda: got1.set_result(None) if not got1.done() else None)
        # second handler reuses the existing self-pipe
        loop.add_signal_handler(signal.SIGUSR2, lambda: got2.set_result(None) if not got2.done() else None)
        signal.raise_signal(signal.SIGUSR1)
        signal.raise_signal(signal.SIGUSR2)
        await asyncio.wait_for(asyncio.gather(got1, got2), 2.0)
        loop.remove_signal_handler(signal.SIGUSR1)
        loop.remove_signal_handler(signal.SIGUSR2)

    run(loop, main())


def test_add_signal_handler_from_non_main_thread(loop: asyncio.AbstractEventLoop) -> None:
    error: list[BaseException] = []

    def worker() -> None:
        try:
            loop.add_signal_handler(signal.SIGUSR1, lambda: None)
        except ValueError as exc:
            error.append(exc)

    async def main() -> None:
        t = threading.Thread(target=worker)
        t.start()
        await loop.run_in_executor(None, t.join)

    run(loop, main())
    assert error and "main thread" in str(error[0])
