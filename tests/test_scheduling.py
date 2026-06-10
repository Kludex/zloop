from __future__ import annotations

import asyncio
import sys
import threading

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


def test_handle_cancel_during_run_does_not_drop_in_flight_call(
    loop: asyncio.AbstractEventLoop,
) -> None:
    # run() snapshots (callback, args, context) with its own references before
    # invoking, so a cancel() that clears and decrefs those fields mid-dispatch
    # (here the callback cancels its own handle) cannot free them under the call.
    seen = []
    box: list[asyncio.Handle] = []

    def cb(value: object) -> None:
        box[0].cancel()
        seen.append(value)

    async def main() -> None:
        box.append(loop.call_soon(cb, "payload"))
        await asyncio.sleep(0)

    run(loop, main())
    assert seen == ["payload"]
    assert box[0].cancelled() is True


@pytest.mark.skipif(
    getattr(sys, "_is_gil_enabled", lambda: True)(),
    reason="the cross-thread Handle race only exists on free-threaded builds",
)
def test_concurrent_cancel_during_dispatch_is_memory_safe(
    loop: asyncio.AbstractEventLoop,
) -> None:
    # Regression: on a free-threaded build, run() reading a Handle's callback/args
    # while another thread's cancel() clears and decrefs them is a use-after-free.
    # Without the critical-section guard this segfaults within a few hundred
    # iterations; the snapshot under the section makes it safe. The cancels race
    # the loop's own dispatch, so "no crash" is the assertion.
    box: dict[str, object] = {}

    def canceller() -> None:
        while not box.get("done"):
            handle = box.get("handle")
            if handle is not None:
                handle.cancel()  # type: ignore[attr-defined]

    async def main() -> None:
        threads = [threading.Thread(target=canceller, daemon=True) for _ in range(3)]
        for thread in threads:
            thread.start()
        for i in range(10_000):
            box["handle"] = loop.call_soon(lambda *a: None, i, "s", b"b", (1, 2))
            await asyncio.sleep(0)
            box["handle"] = None
        box["done"] = True
        for thread in threads:
            thread.join()

    run(loop, main())


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
