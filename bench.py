"""Micro-benchmarks: zloop vs uvloop vs stock asyncio.

Each metric runs REPEATS times per loop; we report the best (least-noisy) result,
which is standard for throughput micro-benchmarks on a busy machine.
"""

from __future__ import annotations

import asyncio
import time

import uvloop

import zloop

REPEATS = 3
N_CALLBACKS = 1_000_000
N_TIMERS = 300_000
N_FUTURES = 200_000
N_ECHO = 40_000
ECHO_PAYLOAD = b"x" * 1024


async def bench_call_soon() -> float:
    """Schedule a batch of callbacks, then run them all - the realistic shape."""
    loop = asyncio.get_running_loop()
    remaining = N_CALLBACKS
    done = loop.create_future()

    def cb() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0:
            done.set_result(None)

    t0 = time.perf_counter()
    for _ in range(N_CALLBACKS):
        loop.call_soon(cb)
    await done
    return N_CALLBACKS / (time.perf_counter() - t0)


async def bench_timers() -> float:
    loop = asyncio.get_running_loop()
    remaining = N_TIMERS
    done = loop.create_future()

    def cb() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0:
            done.set_result(None)

    t0 = time.perf_counter()
    for _ in range(N_TIMERS):
        loop.call_later(0, cb)
    await done
    return N_TIMERS / (time.perf_counter() - t0)


async def bench_futures() -> float:
    """Create + await many futures resolved via call_soon - the create_future path."""
    loop = asyncio.get_running_loop()
    t0 = time.perf_counter()
    for _ in range(N_FUTURES):
        fut = loop.create_future()
        loop.call_soon(fut.set_result, None)
        await fut
    return N_FUTURES / (time.perf_counter() - t0)


async def bench_echo() -> float:
    loop = asyncio.get_running_loop()

    class EchoServer(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]

    server = await loop.create_server(EchoServer, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()
    reader, writer = await asyncio.open_connection(host, port)
    t0 = time.perf_counter()
    for _ in range(N_ECHO):
        writer.write(ECHO_PAYLOAD)
        await writer.drain()
        await reader.readexactly(len(ECHO_PAYLOAD))
    elapsed = time.perf_counter() - t0
    writer.close()
    await writer.wait_closed()
    server.close()
    await server.wait_closed()
    return N_ECHO / elapsed


BENCHES = {
    "call_soon (M/s)": (bench_call_soon, 1e6),
    "call_later (M/s)": (bench_timers, 1e6),
    "create_future (M/s)": (bench_futures, 1e6),
    "echo req/s (k)": (bench_echo, 1e3),
}


async def _best(fn, scale: float) -> float:
    best = 0.0
    for _ in range(REPEATS):
        best = max(best, await fn() / scale)
    return best


async def run_all() -> dict[str, float]:
    return {name: await _best(fn, scale) for name, (fn, scale) in BENCHES.items()}


def main() -> None:
    factories = {"asyncio": None, "uvloop": uvloop.new_event_loop, "zloop": zloop.new_event_loop}
    results: dict[str, dict[str, float]] = {}
    for name, factory in factories.items():
        asyncio.run(run_all(), loop_factory=factory)  # warmup
        results[name] = asyncio.run(run_all(), loop_factory=factory)

    print(f"{'metric':<22}" + "".join(f"{n:>12}" for n in factories) + f"{'zloop/uvloop':>16}")
    print("-" * (22 + 12 * len(factories) + 16))
    for m in BENCHES:
        row = f"{m:<22}"
        for n in factories:
            row += f"{results[n][m]:>12.2f}"
        ratio = results["zloop"][m] / results["uvloop"][m]
        row += f"{ratio:>15.2f}x"
        print(row)


if __name__ == "__main__":
    main()
