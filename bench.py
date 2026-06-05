"""Rough micro-benchmarks: zloop vs uvloop vs stock asyncio.

Not scientific - single machine, warm cache - but enough to see the order of
magnitude. Each loop runs the same coroutine via asyncio.run(loop_factory=...).
"""

from __future__ import annotations

import asyncio
import socket
import time

import uvloop

import zloop

N_CALLBACKS = 1_000_000
N_TIMERS = 200_000
N_ECHO = 50_000
ECHO_PAYLOAD = b"x" * 1024


async def bench_call_soon() -> float:
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    count = 0

    def cb() -> None:
        nonlocal count
        count += 1
        if count >= N_CALLBACKS:
            done.set_result(None)
        else:
            loop.call_soon(cb)

    t0 = time.perf_counter()
    loop.call_soon(cb)
    await done
    return N_CALLBACKS / (time.perf_counter() - t0)


async def bench_timers() -> float:
    loop = asyncio.get_running_loop()
    t0 = time.perf_counter()
    # schedule many zero-delay timers, then let them all fire
    remaining = N_TIMERS
    done = loop.create_future()

    def cb() -> None:
        nonlocal remaining
        remaining -= 1
        if remaining == 0:
            done.set_result(None)

    for _ in range(N_TIMERS):
        loop.call_later(0, cb)
    await done
    return N_TIMERS / (time.perf_counter() - t0)


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


async def run_all() -> dict[str, float]:
    return {
        "call_soon (M/s)": await bench_call_soon() / 1e6,
        "call_later (M/s)": await bench_timers() / 1e6,
        "echo req/s (k)": await bench_echo() / 1e3,
    }


def main() -> None:
    factories = {
        "asyncio": None,
        "uvloop": uvloop.new_event_loop,
        "zloop": zloop.new_event_loop,
    }
    results: dict[str, dict[str, float]] = {}
    for name, factory in factories.items():
        # a warmup run to stabilise
        asyncio.run(run_all(), loop_factory=factory)
        results[name] = asyncio.run(run_all(), loop_factory=factory)

    metrics = list(next(iter(results.values())).keys())
    print(f"{'metric':<20}" + "".join(f"{n:>14}" for n in factories))
    print("-" * (20 + 14 * len(factories)))
    for m in metrics:
        row = f"{m:<20}"
        for n in factories:
            row += f"{results[n][m]:>14.2f}"
        print(row)


if __name__ == "__main__":
    main()
