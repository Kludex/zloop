from __future__ import annotations

import asyncio
import os.path
import socket
import tempfile

import pytest

from tests.conftest import run


async def _roundtrip(loop: asyncio.AbstractEventLoop, host: str, port: int, payload: bytes) -> bytes:
    """Connect with a raw blocking socket in a thread-free way via the loop."""
    fut: asyncio.Future[bytes] = loop.create_future()

    class Client(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            transport.write(payload)  # type: ignore[attr-defined]
            self.transport = transport
            self.buf = bytearray()

        def data_received(self, data: bytes) -> None:
            self.buf += data
            self.transport.close()  # type: ignore[attr-defined]

        def connection_lost(self, exc: BaseException | None) -> None:
            if not fut.done():
                fut.set_result(bytes(self.buf))

    await loop.create_connection(Client, host, port)
    return await asyncio.wait_for(fut, 2.0)


def test_create_server_echo(loop: asyncio.AbstractEventLoop) -> None:
    class Echo(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(b"echo:" + data)  # type: ignore[attr-defined]

    async def main() -> bytes:
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        assert server.is_serving()
        host, port = server.sockets[0].getsockname()
        result = await _roundtrip(loop, host, port, b"hello")
        server.close()
        await server.wait_closed()
        assert not server.is_serving()
        return result

    assert run(loop, main()) == b"echo:hello"


def test_create_server_with_preexisting_socket(loop: asyncio.AbstractEventLoop) -> None:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    host, port = sock.getsockname()

    class Echo(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]

    async def main() -> bytes:
        server = await loop.create_server(Echo, sock=sock)
        result = await _roundtrip(loop, host, port, b"x")
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"x"


def test_accept_caps_burst_then_reschedules_to_drain(loop: asyncio.AbstractEventLoop) -> None:
    # _accept caps each readiness at backlog + 1 accepts so a connection flood
    # can't starve the loop, then reschedules itself (not relying on a readiness
    # re-fire, which the edge-triggered io_uring poll backend would not give) to
    # take the rest. A fake listener with a fixed-size queue exercises both: the
    # per-burst cap, and the cross-iteration draining of the remainder.
    backlog = 3
    queued = (backlog + 1) * 3 + 2  # several capped bursts plus a partial one

    async def main() -> tuple[int, int]:
        accepted = 0

        class Counter(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                nonlocal accepted
                accepted += 1

        server = await loop.create_server(Counter, "127.0.0.1", 0, backlog=backlog)

        pairs: list[tuple[socket.socket, socket.socket]] = []
        remaining = queued

        class FakeListener:
            def accept(self) -> tuple[socket.socket, tuple[str, int]]:
                nonlocal remaining
                if remaining <= 0:
                    raise BlockingIOError
                remaining -= 1
                a, b = socket.socketpair()
                pairs.append((a, b))
                return a, ("127.0.0.1", 0)

        # The first call caps at backlog + 1 (the burst limit), then reschedules
        # itself; pumping the loop drains the rest across iterations.
        server._accept(FakeListener())  # type: ignore[arg-type]
        first_burst = accepted
        for _ in range(20):
            if remaining <= 0:
                break
            await asyncio.sleep(0)

        for a, b in pairs:
            a.close()
            b.close()
        server.close()
        await server.wait_closed()
        return first_burst, accepted

    first_burst, total = run(loop, main())
    assert first_burst == backlog + 1  # one readiness accepts at most the cap
    assert total == queued  # the rest are drained via reschedule, none dropped


def test_accept_does_not_reschedule_after_close(loop: asyncio.AbstractEventLoop) -> None:
    # A capped _accept that finds the server already closed must not reschedule
    # itself (it would accept onto a torn-down server). Closing flips _active off.
    async def main() -> int:
        accepted = 0

        class Counter(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                nonlocal accepted
                accepted += 1

        server = await loop.create_server(Counter, "127.0.0.1", 0, backlog=2)

        pairs: list[tuple[socket.socket, socket.socket]] = []

        class AlwaysReady:
            def accept(self) -> tuple[socket.socket, tuple[str, int]]:
                a, b = socket.socketpair()
                pairs.append((a, b))
                return a, ("127.0.0.1", 0)

        server.close()  # _active is now False
        server._accept(AlwaysReady())  # type: ignore[arg-type]
        await asyncio.sleep(0)  # any erroneous reschedule would run here

        for a, b in pairs:
            a.close()
            b.close()
        await server.wait_closed()
        return accepted

    # Exactly one capped burst, and no further accepts from a reschedule.
    assert run(loop, main()) == 3


def test_server_get_loop(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        assert server.get_loop() is loop
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_create_server_not_serving_until_started(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, start_serving=False)
        assert not server.is_serving()
        await server.start_serving()
        assert server.is_serving()
        # idempotent
        await server.start_serving()
        server.close()
        # second close is a no-op
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_create_server_bind_error(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(OSError):
            # An address not assigned to any local interface can never be bound,
            # regardless of privilege (privileged ports succeed when run as root).
            await loop.create_server(asyncio.Protocol, "192.0.2.1", 0)

    run(loop, main())


def test_create_server_multiple_hosts(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, ["127.0.0.1"], 0)
        assert len(server.sockets) >= 1
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_create_unix_server(loop: asyncio.AbstractEventLoop) -> None:
    # AF_UNIX paths are limited to ~104 bytes, so avoid pytest's long tmp_path.
    tmpdir = tempfile.mkdtemp()
    path = str(os.path.join(tmpdir, "z.sock"))

    class Echo(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]

    async def main() -> bytes:
        server = await loop.create_unix_server(Echo, path)
        fut: asyncio.Future[bytes] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                transport.write(b"unix")  # type: ignore[attr-defined]
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                fut.set_result(data)
                self.transport.close()  # type: ignore[attr-defined]

        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.connect(path)
        sock.setblocking(False)
        await loop.create_connection(Client, sock=sock)
        result = await asyncio.wait_for(fut, 2.0)
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"unix"


def test_create_unix_server_requires_path_or_sock(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(ValueError):
            await loop.create_unix_server(asyncio.Protocol)

    run(loop, main())


def test_create_unix_server_with_sock_and_deferred_start(loop: asyncio.AbstractEventLoop) -> None:
    path = os.path.join(tempfile.mkdtemp(), "z.sock")
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.bind(path)

    async def main() -> None:
        server = await loop.create_unix_server(asyncio.Protocol, sock=sock, start_serving=False)
        assert not server.is_serving()
        await server.start_serving()
        assert server.is_serving()
        server.close()
        await server.wait_closed()

    run(loop, main())
