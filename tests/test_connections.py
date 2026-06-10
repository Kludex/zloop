from __future__ import annotations

import asyncio
import os
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


def test_create_connection_large_payload_roundtrip(loop: asyncio.AbstractEventLoop) -> None:
    # A payload several times READ_CHUNK (256 KiB) exercises the read path's drain
    # loop across many recv() calls rather than the single-read fast case.
    payload = b"x" * (1024 * 1024)

    async def main() -> bytes:
        server, host, port = await _echo_server(loop)
        got: asyncio.Future[bytes] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.buf = bytearray()
                transport.write(payload)  # type: ignore[attr-defined]

            def data_received(self, data: bytes) -> None:
                self.buf.extend(data)
                if len(self.buf) >= len(payload) and not got.done():
                    got.set_result(bytes(self.buf))

        transport, protocol = await loop.create_connection(Client, host, port)
        result = await asyncio.wait_for(got, 5.0)
        transport.close()
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == payload


def test_alternating_large_and_small_messages(loop: asyncio.AbstractEventLoop) -> None:
    # Request/response with sizes that cross the completion backend's adaptive
    # recv thresholds in both directions: a large message escalates a connection
    # from the ring to owned-buffer recvs, a run of small ones reverts it, and a
    # second large message escalates it again. Every byte must survive the
    # strategy transitions (and on readiness backends this is just an echo test).
    messages = [b"L" * (300 * 1024)] + [b"s" * 64] * 8 + [b"M" * (300 * 1024)] + [b"t" * 64] * 8

    async def main() -> tuple[list[bytes], int | None]:
        server, host, port = await _echo_server(loop)
        received: list[bytes] = []
        done: asyncio.Future[None] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport
                self.buf = bytearray()
                self.idx = 0
                transport.write(messages[0])  # type: ignore[attr-defined]

            def data_received(self, data: bytes) -> None:
                self.buf.extend(data)
                if len(self.buf) < len(messages[self.idx]):
                    return
                received.append(bytes(self.buf[: len(messages[self.idx])]))
                del self.buf[: len(messages[self.idx])]
                self.idx += 1
                if self.idx == len(messages):
                    done.set_result(None)
                else:
                    self.transport.write(messages[self.idx])  # type: ignore[attr-defined]

        transport, protocol = await loop.create_connection(Client, host, port)
        await asyncio.wait_for(done, 10.0)
        switches = transport.get_extra_info("zloop_recv_switches")
        transport.close()
        server.close()
        await server.wait_closed()
        return received, switches

    received, switches = run(loop, main())
    assert received == messages
    # On the adaptive completion backend (Linux, ZLOOP_IO_URING=completion) the
    # client transport must have escalated to owned recvs for each large echo and
    # reverted on the small runs: ring->owned, owned->ring, ring->owned at least.
    # Readiness backends (and ZLOOP_IO_URING=owned) report no switches.
    if switches is not None and os.environ.get("ZLOOP_IO_URING") == "completion":
        assert switches >= 3


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


def test_create_connection_closes_socket_on_factory_error(loop: asyncio.AbstractEventLoop) -> None:
    server_holder: list = []

    class Srv(asyncio.Protocol):
        def connection_made(self, t: asyncio.BaseTransport) -> None:
            pass

    def bad_factory():
        raise RuntimeError("factory boom")

    async def main() -> None:
        server, host, port = await _echo_server(loop)
        with pytest.raises(RuntimeError, match="factory boom"):
            await loop.create_connection(bad_factory, host, port)
        server.close()
        await server.wait_closed()
        server_holder.append(server)

    run(loop, main())


def test_accepted_connection_factory_error_does_not_leak(loop: asyncio.AbstractEventLoop) -> None:
    errors: list = []

    def bad_factory():
        raise RuntimeError("server factory boom")

    async def main() -> None:
        loop.set_exception_handler(lambda lp, ctx: errors.append(ctx))
        server = await loop.create_server(bad_factory, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()
        # connect a client; the server's protocol_factory will raise on accept
        c = socket.create_connection((host, port))
        await asyncio.sleep(0.05)
        c.close()
        server.close()
        await server.wait_closed()

    run(loop, main())


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


def test_create_connection_local_addr(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> tuple:
        server, host, port = await _echo_server(loop)
        got: asyncio.Future[tuple] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                if not got.done():
                    got.set_result(transport.get_extra_info("sockname"))
                transport.close()

        await loop.create_connection(Client, host, port, local_addr=("127.0.0.1", 0))
        sockname = await asyncio.wait_for(got, 2.0)
        server.close()
        await server.wait_closed()
        return sockname

    sockname = run(loop, main())
    assert sockname[0] == "127.0.0.1"


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


def test_pause_resume_reading_inside_data_received(loop: asyncio.AbstractEventLoop) -> None:
    # Reentrant pause+resume while a recv completion is being delivered: the
    # resume arms a fresh recv, and the completion handler must not arm a second
    # one on top - every message still arrives exactly once, in order.
    messages = [b"one", b"two", b"three"]

    async def main() -> None:
        received: list[bytes] = []
        done: asyncio.Future[None] = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                self.transport = transport
                self.buf = bytearray()

            def data_received(self, data: bytes) -> None:
                self.transport.pause_reading()
                self.transport.resume_reading()
                self.buf.extend(data)
                self.transport.write(data)
                if len(self.buf) == sum(len(m) for m in messages):
                    self.buf.clear()

        server = await loop.create_server(Server, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                self.transport = transport
                self.idx = 0
                self.buf = bytearray()
                transport.write(messages[0])

            def data_received(self, data: bytes) -> None:
                self.buf.extend(data)
                if len(self.buf) < len(messages[self.idx]):
                    return
                received.append(bytes(self.buf[: len(messages[self.idx])]))
                del self.buf[: len(messages[self.idx])]
                self.idx += 1
                if self.idx == len(messages):
                    done.set_result(None)
                else:
                    self.transport.write(messages[self.idx])

        await loop.create_connection(Client, host, port)
        await asyncio.wait_for(done, 5.0)
        assert received == messages
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_set_protocol_reroutes_data_received(loop: asyncio.AbstractEventLoop) -> None:
    # The transport caches the bound data_received; set_protocol must rebind it so
    # data goes to the new protocol, not the original one.
    async def main() -> None:
        server, host, port = await _echo_server(loop)

        first: list[bytes] = []
        second: list[bytes] = []
        got: asyncio.Future[None] = loop.create_future()

        class First(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                first.append(data)

        class Second(asyncio.Protocol):
            def data_received(self, data: bytes) -> None:
                second.append(data)
                if not got.done():
                    got.set_result(None)

        transport, proto = await loop.create_connection(First, host, port)
        transport.set_protocol(Second())
        transport.write(b"after-swap")
        await asyncio.wait_for(got, 2.0)

        assert b"".join(second) == b"after-swap"
        assert first == []  # the original protocol must not receive post-swap data
        transport.close()
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_set_protocol_from_within_data_received(loop: asyncio.AbstractEventLoop) -> None:
    # Reentrant rebind: data_received swaps the protocol mid-callback, which clears
    # the transport's cached bound data_received. The in-flight call holds its own
    # reference so the method is not freed under itself (a latent UAF the cached
    # path would otherwise risk; deterministic only under a sanitizer build).
    async def main() -> None:
        server, host, port = await _echo_server(loop)
        done: asyncio.Future[None] = loop.create_future()

        class Swapper(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                self.transport.set_protocol(asyncio.Protocol())  # clears the cache
                if not done.done():
                    done.set_result(None)

        transport, _ = await loop.create_connection(Swapper, host, port)
        transport.write(b"trigger")
        await asyncio.wait_for(done, 2.0)
        transport.close()
        server.close()
        await server.wait_closed()

    run(loop, main())
