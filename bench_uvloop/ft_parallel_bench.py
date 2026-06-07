"""Throwaway free-threaded parallel-loops benchmark: N zloop loops on N threads,
aggregate echo throughput, epoll vs io_uring. Requires GIL actually off.

WARNING: zloop's free-threading races are NOT fixed yet; this runs under
PYTHON_GIL=0 on racy code. Numbers are an indicative signal only and the process
may crash or produce corrupt results.
"""

from __future__ import annotations

import asyncio
import socket
import sys
import threading
import time

import zloop


def run_loop_workload(n_conns: int, msg: int, dur: float, out: list, idx: int) -> None:
    """One OS thread: its own zloop loop running an in-process echo client/server."""

    async def main() -> int:
        done = 0

        class Echo(asyncio.Protocol):
            def connection_made(self, t):
                self.t = t

            def data_received(self, data):
                self.t.write(data)

        loop = asyncio.get_running_loop()
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()
        payload = b"x" * msg

        async def client():
            nonlocal done
            r, w = await asyncio.open_connection(host, port)
            buf = b""
            deadline = time.monotonic() + dur
            while time.monotonic() < deadline:
                w.write(payload)
                await w.drain()
                buf = b""
                while len(buf) < msg:
                    chunk = await r.read(msg - len(buf))
                    if not chunk:
                        return
                    buf += chunk
                done += 1
            w.close()

        await asyncio.gather(*[client() for _ in range(n_conns)])
        server.close()
        return done

    runner = asyncio.Runner(loop_factory=zloop.new_event_loop)
    try:
        out[idx] = runner.run(main())
    except Exception as e:  # racy code; record instead of killing the whole run
        out[idx] = f"ERR:{type(e).__name__}:{e}"
    finally:
        runner.close()


def main() -> int:
    n_threads = int(sys.argv[1]) if len(sys.argv) > 1 else 4
    n_conns = int(sys.argv[2]) if len(sys.argv) > 2 else 8
    msg = int(sys.argv[3]) if len(sys.argv) > 3 else 1024
    dur = float(sys.argv[4]) if len(sys.argv) > 4 else 3.0

    gil = sys._is_gil_enabled() if hasattr(sys, "_is_gil_enabled") else None
    print(f"GIL enabled: {gil}  threads={n_threads} conns/thread={n_conns} msg={msg}B dur={dur}s")

    out: list = [None] * n_threads
    threads = [
        threading.Thread(target=run_loop_workload, args=(n_conns, msg, dur, out, i))
        for i in range(n_threads)
    ]
    t0 = time.monotonic()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    elapsed = time.monotonic() - t0

    errs = [r for r in out if isinstance(r, str)]
    oks = [r for r in out if isinstance(r, int)]
    total = sum(oks)
    print(f"  per-thread roundtrips: {out}")
    if errs:
        print(f"  ERRORS in {len(errs)}/{n_threads} threads (racy code): {errs[:2]}")
    print(f"  aggregate: {total} roundtrips in {elapsed:.2f}s => {total/elapsed:,.0f} req/s")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
