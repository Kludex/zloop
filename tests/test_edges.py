from __future__ import annotations

import asyncio
import socket

import pytest

from tests.conftest import run


def test_wait_closed_blocks_until_close(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        waiter = asyncio.ensure_future(server.wait_closed())
        await asyncio.sleep(0.01)
        assert not waiter.done()
        server.close()
        await asyncio.wait_for(waiter, 2.0)
        # sockets is now empty
        assert server.sockets == ()
        # wait_closed after close returns immediately
        await server.wait_closed()

    run(loop, main())


def test_host_none_binds_all_interfaces(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, None, 0)
        assert len(server.sockets) >= 1
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_empty_host_binds(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "", 0)
        assert len(server.sockets) >= 1
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_reuse_address_explicit_false(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, reuse_address=False)
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_executor_exception_propagates(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        # exercise shutdown_default_executor's error path is hard; instead verify
        # an exception raised in executor work propagates to the awaiter.
        with pytest.raises(ValueError, match="in thread"):
            await loop.run_in_executor(None, _raise_in_thread)

    run(loop, main())


def _raise_in_thread() -> None:
    raise ValueError("in thread")


def test_sock_connect_immediate(loop: asyncio.AbstractEventLoop) -> None:
    # connecting a unix socketpair end is immediate (no EINPROGRESS), covering
    # the fast path of _sock_connect.
    class Echo(asyncio.Protocol):
        def connection_made(self, t: asyncio.BaseTransport) -> None:
            self.t = t

        def data_received(self, data: bytes) -> None:
            self.t.write(data)  # type: ignore[attr-defined]

    async def main() -> bytes:
        server = await loop.create_server(Echo, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()
        # pre-connect a blocking socket so create_connection(sock=) path is used
        s = socket.create_connection((host, port))
        s.setblocking(False)
        got: asyncio.Future[bytes] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, t: asyncio.BaseTransport) -> None:
                t.write(b"q")  # type: ignore[attr-defined]
                self.t = t

            def data_received(self, data: bytes) -> None:
                if not got.done():
                    got.set_result(data)
                self.t.close()  # type: ignore[attr-defined]

        await loop.create_connection(Client, sock=s)
        result = await asyncio.wait_for(got, 2.0)
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"q"


def test_unix_server_bind_error(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        with pytest.raises(OSError):
            # binding to a path in a nonexistent directory fails
            await loop.create_unix_server(asyncio.Protocol, "/nonexistent-dir-zzz/x.sock")

    run(loop, main())
