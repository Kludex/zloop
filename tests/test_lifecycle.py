from __future__ import annotations

import asyncio

import pytest

import zloop
from tests.conftest import run


def test_run_until_complete_with_plain_coroutine(loop: asyncio.AbstractEventLoop) -> None:
    async def coro() -> str:
        return "value"

    assert loop.run_until_complete(coro()) == "value"


def test_run_until_complete_propagates_exception(loop: asyncio.AbstractEventLoop) -> None:
    async def coro() -> None:
        raise ValueError("propagated")

    with pytest.raises(ValueError, match="propagated"):
        loop.run_until_complete(coro())


def test_is_running_false_outside(loop: asyncio.AbstractEventLoop) -> None:
    assert loop.is_running() is False


def test_is_running_true_inside(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bool:
        return loop.is_running()

    assert run(loop, main()) is True


def test_cannot_run_while_running(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(RuntimeError):
            loop.run_until_complete(asyncio.sleep(0))

    run(loop, main())


def test_run_until_complete_stopped_before_done(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        # stop the loop from inside without completing the awaited future
        loop.call_soon(loop.stop)
        await asyncio.Future()  # never resolves

    with pytest.raises(RuntimeError, match="stopped before Future completed"):
        loop.run_until_complete(main())


def test_close_idempotent(loop: asyncio.AbstractEventLoop) -> None:
    assert loop.is_closed() is False
    loop.close()
    assert loop.is_closed() is True
    loop.close()  # no-op
    assert loop.is_closed() is True


def test_cannot_close_running_loop(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(RuntimeError):
            loop.close()

    run(loop, main())


def test_closed_loop_rejects_scheduling(loop: asyncio.AbstractEventLoop) -> None:
    loop.close()
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        loop.call_soon(lambda: None)
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        loop.call_later(1, lambda: None)
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        loop.call_at(loop.time() + 1, lambda: None)
    with pytest.raises(RuntimeError, match="Event loop is closed"):
        loop.call_soon_threadsafe(lambda: None)


def test_closed_loop_rejects_add_reader(loop: asyncio.AbstractEventLoop) -> None:
    import socket

    s = socket.socket()
    loop.close()
    try:
        with pytest.raises(RuntimeError, match="Event loop is closed"):
            loop.add_reader(s.fileno(), lambda: None)
    finally:
        s.close()


def test_add_reader_rejects_bad_fd(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(ValueError):
            loop.add_reader(-1, lambda: None)

    run(loop, main())


def test_call_later_nan_inf_does_not_crash(loop: asyncio.AbstractEventLoop) -> None:
    fired: list[str] = []

    async def main() -> None:
        loop.call_later(float("nan"), fired.append, "nan")
        loop.call_later(float("inf"), fired.append, "inf")
        loop.call_later(0, fired.append, "zero")
        await asyncio.sleep(0.02)

    run(loop, main())
    # NaN is treated as fire-immediately; inf never fires within the window.
    assert "nan" in fired and "zero" in fired and "inf" not in fired


def test_debug_flag(loop: asyncio.AbstractEventLoop) -> None:
    assert loop.get_debug() is False
    loop.set_debug(True)
    assert loop.get_debug() is True
    loop.set_debug(False)
    assert loop.get_debug() is False


def test_create_future(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> int:
        fut = loop.create_future()
        assert isinstance(fut, asyncio.Future)
        loop.call_soon(fut.set_result, 5)
        return await fut

    assert run(loop, main()) == 5


def test_create_task(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> int:
        async def inner() -> int:
            return 11

        task = loop.create_task(inner())
        assert isinstance(task, asyncio.Task)
        return await task

    assert run(loop, main()) == 11


def test_task_factory(loop: asyncio.AbstractEventLoop) -> None:
    created: list[object] = []

    def factory(lp, coro, **kwargs):
        created.append(coro)
        return asyncio.Task(coro, loop=lp)

    assert loop.get_task_factory() is None

    async def main() -> int:
        loop.set_task_factory(factory)
        assert loop.get_task_factory() is factory

        async def inner() -> int:
            return 3

        result = await loop.create_task(inner())
        loop.set_task_factory(None)
        return result

    assert run(loop, main()) == 3
    # only the explicit create_task(inner()) used the factory; main() ran before
    # the factory was installed.
    assert len(created) == 1


def test_set_task_factory_must_be_callable_or_none(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        loop.set_task_factory(42)  # type: ignore[arg-type]
    loop.set_task_factory(None)
    assert loop.get_task_factory() is None


def test_call_soon_threadsafe(loop: asyncio.AbstractEventLoop) -> None:
    import threading

    async def main() -> str:
        fut = loop.create_future()

        def from_thread() -> None:
            loop.call_soon_threadsafe(fut.set_result, "threadsafe")

        threading.Thread(target=from_thread).start()
        return await asyncio.wait_for(fut, 2.0)

    assert run(loop, main()) == "threadsafe"


def test_call_soon_threadsafe_requires_callable(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        loop.call_soon_threadsafe(123)


def test_call_at_requires_callable(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        loop.call_at(loop.time() + 1, 123)


def test_call_later_requires_callable(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        loop.call_later(0.1, 123)


def test_add_reader_writer(loop: asyncio.AbstractEventLoop) -> None:
    import socket

    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)

    async def main() -> bytes:
        got: asyncio.Future[bytes] = loop.create_future()

        def on_read() -> None:
            data = a.recv(1024)
            if not got.done():
                got.set_result(data)

        loop.add_reader(a.fileno(), on_read)
        loop.add_writer(b.fileno(), lambda: None)
        loop.remove_writer(b.fileno())
        b.send(b"hello")
        result = await asyncio.wait_for(got, 2.0)
        loop.remove_reader(a.fileno())
        return result

    try:
        assert run(loop, main()) == b"hello"
    finally:
        a.close()
        b.close()


def test_remove_reader_unregistered(loop: asyncio.AbstractEventLoop) -> None:
    import socket

    s = socket.socket()

    async def main() -> bool:
        return loop.remove_reader(s.fileno())

    try:
        assert run(loop, main()) is False
    finally:
        s.close()


def test_new_event_loop_helper() -> None:
    lp = zloop.new_event_loop()
    assert isinstance(lp, asyncio.AbstractEventLoop)
    lp.close()
