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
