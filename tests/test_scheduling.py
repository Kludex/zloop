from __future__ import annotations

import asyncio

import pytest

import zloop
from tests.conftest import run


def test_run_until_complete_returns_result(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> int:
        return 42

    assert run(loop, main()) == 42


def test_is_instance_of_abstract_event_loop(loop: asyncio.AbstractEventLoop) -> None:
    assert isinstance(loop, asyncio.AbstractEventLoop)


def test_new_event_loop_factory_returns_fresh_loops() -> None:
    a = zloop.new_event_loop()
    b = zloop.new_event_loop()
    assert a is not b
    a.close()
    b.close()


def test_get_running_loop_inside(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bool:
        return asyncio.get_running_loop() is loop

    assert run(loop, main()) is True


def test_time_is_monotonic(loop: asyncio.AbstractEventLoop) -> None:
    assert loop.time() <= loop.time()


def test_call_soon_runs_in_fifo(loop: asyncio.AbstractEventLoop) -> None:
    order: list[int] = []

    async def main() -> None:
        loop.call_soon(order.append, 1)
        loop.call_soon(order.append, 2)
        loop.call_soon(order.append, 3)
        await asyncio.sleep(0)

    run(loop, main())
    assert order == [1, 2, 3]


def test_call_soon_returns_handle_that_cancels(loop: asyncio.AbstractEventLoop) -> None:
    ran = []

    async def main() -> None:
        h = loop.call_soon(ran.append, 1)
        assert h.cancelled() is False
        h.cancel()
        assert h.cancelled() is True
        await asyncio.sleep(0)

    run(loop, main())
    assert ran == []


def test_call_soon_requires_callable(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        loop.call_soon(123)


def test_sleep_delays(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> float:
        t0 = loop.time()
        await asyncio.sleep(0.05)
        return loop.time() - t0

    dt = run(loop, main())
    assert dt >= 0.045


def test_call_later_fires(loop: asyncio.AbstractEventLoop) -> None:
    fired: list[str] = []

    async def main() -> None:
        loop.call_later(0.01, fired.append, "late")
        await asyncio.sleep(0.05)

    run(loop, main())
    assert fired == ["late"]


def test_call_later_negative_delay_runs_soon(loop: asyncio.AbstractEventLoop) -> None:
    fired: list[str] = []

    async def main() -> None:
        loop.call_later(-1, fired.append, "now")
        await asyncio.sleep(0)

    run(loop, main())
    assert fired == ["now"]


def test_timer_handle_when_and_cancel(loop: asyncio.AbstractEventLoop) -> None:
    fired: list[str] = []

    async def main() -> None:
        h = loop.call_later(0.01, fired.append, "x")
        assert isinstance(h.when(), float)
        h.cancel()
        await asyncio.sleep(0.03)

    run(loop, main())
    assert fired == []


def test_call_at_absolute(loop: asyncio.AbstractEventLoop) -> None:
    fired: list[str] = []

    async def main() -> None:
        loop.call_at(loop.time() + 0.01, fired.append, "at")
        await asyncio.sleep(0.03)

    run(loop, main())
    assert fired == ["at"]


def test_gather_concurrency(loop: asyncio.AbstractEventLoop) -> None:
    async def work(n: int) -> int:
        await asyncio.sleep(0.01 * n)
        return n

    async def main() -> list[int]:
        return await asyncio.gather(work(3), work(1), work(2))

    assert run(loop, main()) == [3, 1, 2]
