from __future__ import annotations

import asyncio

from pytest_codspeed import BenchmarkFixture

import zloop


def test_call_soon(benchmark: BenchmarkFixture) -> None:
    loop = zloop.new_event_loop()

    def schedule_and_drain() -> None:
        for _ in range(1000):
            loop.call_soon(lambda: None)
        loop.run_until_complete(asyncio.sleep(0))

    try:
        benchmark(schedule_and_drain)
    finally:
        loop.close()


def test_call_later_cancel(benchmark: BenchmarkFixture) -> None:
    loop = zloop.new_event_loop()

    def schedule_and_cancel() -> None:
        handles = [loop.call_later(10, lambda: None) for _ in range(1000)]
        for handle in handles:
            handle.cancel()

    try:
        benchmark(schedule_and_cancel)
    finally:
        loop.close()


def test_sleep_zero(benchmark: BenchmarkFixture) -> None:
    loop = zloop.new_event_loop()

    async def main() -> None:
        for _ in range(1000):
            await asyncio.sleep(0)

    try:
        benchmark(lambda: loop.run_until_complete(main()))
    finally:
        loop.close()


def test_gather(benchmark: BenchmarkFixture) -> None:
    loop = zloop.new_event_loop()

    async def noop() -> None:
        await asyncio.sleep(0)

    async def main() -> None:
        await asyncio.gather(*(noop() for _ in range(100)))

    try:
        benchmark(lambda: loop.run_until_complete(main()))
    finally:
        loop.close()


def test_run_until_complete(benchmark: BenchmarkFixture) -> None:
    loop = zloop.new_event_loop()

    async def main() -> int:
        return 42

    try:
        benchmark(lambda: loop.run_until_complete(main()))
    finally:
        loop.close()
