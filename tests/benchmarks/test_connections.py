from __future__ import annotations

import asyncio

from pytest_codspeed import BenchmarkFixture

import zloop

PAYLOAD = b"x" * 1024
ROUNDS = 200


def test_echo_roundtrip(benchmark: BenchmarkFixture) -> None:
    loop = zloop.new_event_loop()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(len(PAYLOAD)):
            writer.write(data)
            await writer.drain()
        writer.close()

    async def echo() -> None:
        server = await asyncio.start_server(handle, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()[:2]
        reader, writer = await asyncio.open_connection(host, port)
        for _ in range(ROUNDS):
            writer.write(PAYLOAD)
            await writer.drain()
            await reader.readexactly(len(PAYLOAD))
        writer.close()
        await writer.wait_closed()
        server.close()
        await server.wait_closed()

    try:
        benchmark(lambda: loop.run_until_complete(echo()))
    finally:
        loop.close()
