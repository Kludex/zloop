from __future__ import annotations

import asyncio
import socket

import pytest

from tests.conftest import run


async def _echo_server(loop: asyncio.AbstractEventLoop) -> tuple[asyncio.AbstractServer, str, int]:
    class Echo(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(data)  # type: ignore[attr-defined]
            if data == b"CLOSE":
                self.transport.close()  # type: ignore[attr-defined]

    server = await loop.create_server(Echo, "127.0.0.1", 0)
    host, port = server.sockets[0].getsockname()
    return server, host, port


def test_create_connection_roundtrip(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        server, host, port = await _echo_server(loop)
        got: asyncio.Future[bytes] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport
                transport.write(b"ping")  # type: ignore[attr-defined]

            def data_received(self, data: bytes) -> None:
                if not got.done():
                    got.set_result(data)
                self.transport.close()  # type: ignore[attr-defined]

        transport, protocol = await loop.create_connection(Client, host, port)
        assert transport.get_extra_info("peername")[1] == port
        result = await asyncio.wait_for(got, 2.0)
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"ping"


def test_create_connection_with_sock(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        server, host, port = await _echo_server(loop)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.setblocking(False)
        got: asyncio.Future[bytes] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                transport.write(b"viasock")  # type: ignore[attr-defined]
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                if not got.done():
                    got.set_result(data)
                self.transport.close()  # type: ignore[attr-defined]

        await loop.create_connection(Client, sock=sock)
        result = await asyncio.wait_for(got, 2.0)
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"viasock"


def test_create_connection_refused(loop: asyncio.AbstractEventLoop) -> None:
    # find a closed port by binding then closing
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    _, port = s.getsockname()
    s.close()

    async def main() -> None:
        with pytest.raises(OSError):
            await loop.create_connection(asyncio.Protocol, "127.0.0.1", port)

    run(loop, main())


def test_transport_writelines(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        server, host, port = await _echo_server(loop)
        got: asyncio.Future[bytes] = loop.create_future()
        buf = bytearray()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                transport.writelines([b"aa", b"bb", b"cc"])  # type: ignore[attr-defined]
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                buf.extend(data)
                if len(buf) >= 6:
                    if not got.done():
                        got.set_result(bytes(buf))
                    self.transport.close()  # type: ignore[attr-defined]

        await loop.create_connection(Client, host, port)
        result = await asyncio.wait_for(got, 2.0)
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"aabbcc"


def test_transport_attributes_and_protocol(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server, host, port = await _echo_server(loop)
        done: asyncio.Future[None] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                assert transport.get_protocol() is self
                assert transport.can_write_eof() is True
                assert transport.is_closing() is False
                assert transport.get_write_buffer_size() == 0
                low, high = transport.get_write_buffer_limits()
                assert high >= low
                transport.set_write_buffer_limits(high=2048, low=512)
                assert transport.get_write_buffer_limits() == (512, 2048)
                assert transport.get_extra_info("nonexistent", "dflt") == "dflt"
                transport.close()

            def connection_lost(self, exc: BaseException | None) -> None:
                if not done.done():
                    done.set_result(None)

        await loop.create_connection(Client, host, port)
        await asyncio.wait_for(done, 2.0)
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_write_after_write_eof_raises(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server, host, port = await _echo_server(loop)
        errs: list[type] = []

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                transport.write_eof()
                try:
                    transport.write(b"after eof")
                except RuntimeError:
                    errs.append(RuntimeError)
                try:
                    transport.writelines([b"x"])
                except RuntimeError:
                    errs.append(RuntimeError)
                transport.close()
                self.transport = transport

        await loop.create_connection(Client, host, port)
        await asyncio.sleep(0.05)
        server.close()
        await server.wait_closed()
        assert errs == [RuntimeError, RuntimeError]

    run(loop, main())


def test_set_write_buffer_limits_validation(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server, host, port = await _echo_server(loop)
        results: dict[str, object] = {}

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                # high < low is invalid
                try:
                    transport.set_write_buffer_limits(high=10, low=100)
                except ValueError:
                    results["invalid"] = True
                # negative is invalid
                try:
                    transport.set_write_buffer_limits(high=-1)
                except ValueError:
                    results["negative"] = True
                # only-low derives high = 4*low
                transport.set_write_buffer_limits(low=100)
                results["only_low"] = transport.get_write_buffer_limits()
                # only-high derives low = high//4
                transport.set_write_buffer_limits(high=400)
                results["only_high"] = transport.get_write_buffer_limits()
                transport.close()
                self.transport = transport

        await loop.create_connection(Client, host, port)
        await asyncio.sleep(0.05)
        server.close()
        await server.wait_closed()
        assert results["invalid"] is True
        assert results["negative"] is True
        assert results["only_low"] == (100, 400)
        assert results["only_high"] == (100, 400)

    run(loop, main())


def test_transport_write_eof(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        eof_seen: asyncio.Future[None] = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def eof_received(self) -> bool:
                if not eof_seen.done():
                    eof_seen.set_result(None)
                return False

        server = await loop.create_server(Server, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                transport.write(b"data")
                transport.write_eof()
                self.transport = transport

        await loop.create_connection(Client, host, port)
        await asyncio.wait_for(eof_seen, 2.0)
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_transport_abort(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        lost: asyncio.Future[None] = loop.create_future()
        server, host, port = await _echo_server(loop)

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                transport.write(b"x")
                transport.abort()
                self.transport = transport

            def connection_lost(self, exc: BaseException | None) -> None:
                if not lost.done():
                    lost.set_result(None)

        await loop.create_connection(Client, host, port)
        await asyncio.wait_for(lost, 2.0)
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_pause_resume_reading(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        received: list[bytes] = []
        ready: asyncio.Future[None] = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                self.transport = transport
                transport.pause_reading()
                transport.resume_reading()

            def data_received(self, data: bytes) -> None:
                received.append(data)
                if not ready.done():
                    ready.set_result(None)

        server = await loop.create_server(Server, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                transport.write(b"payload")
                self.transport = transport

        await loop.create_connection(Client, host, port)
        await asyncio.wait_for(ready, 2.0)
        assert b"".join(received) == b"payload"
        server.close()
        await server.wait_closed()

    run(loop, main())
