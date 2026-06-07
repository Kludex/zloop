from __future__ import annotations

import asyncio
import socket
import ssl

import pytest

from tests.conftest import run

trustme = pytest.importorskip("trustme")


@pytest.fixture
def tls_certs():
    ca = trustme.CA()
    cert = ca.issue_cert("localhost", "127.0.0.1")
    return ca, cert


def _server_ctx(cert) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    cert.configure_cert(ctx)
    return ctx


def _client_ctx(ca) -> ssl.SSLContext:
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ca.configure_trust(ctx)
    return ctx


def _listener() -> socket.socket:
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    sock.setblocking(False)
    return sock


async def _accept(loop: asyncio.AbstractEventLoop, listener: socket.socket) -> socket.socket:
    accepted: asyncio.Future[socket.socket] = loop.create_future()

    def on_acceptable() -> None:
        conn, _ = listener.accept()
        loop.remove_reader(listener.fileno())
        accepted.set_result(conn)

    loop.add_reader(listener.fileno(), on_acceptable)
    return await accepted


def test_connect_accepted_socket_plain(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        listener = _listener()
        got: asyncio.Future[bytes] = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                if not got.done():
                    got.set_result(data)

        client = socket.socket()
        client.setblocking(False)
        try:
            client.connect(listener.getsockname())
        except BlockingIOError:
            pass
        conn = await _accept(loop, listener)
        transport, protocol = await loop.connect_accepted_socket(Server, conn)
        assert transport.get_extra_info("socket") is conn
        assert isinstance(protocol, Server)
        client.setblocking(True)
        client.sendall(b"hi")
        result = await asyncio.wait_for(got, 5.0)
        transport.close()
        client.close()
        listener.close()
        return result

    assert run(loop, main()) == b"hi"


def test_connect_accepted_socket_tls(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    ca, cert = tls_certs
    server_ctx = _server_ctx(cert)
    client_ctx = _client_ctx(ca)

    async def main() -> bytes:
        listener = _listener()
        host, port = listener.getsockname()
        got: asyncio.Future[bytes] = loop.create_future()

        class Server(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                if not got.done():
                    got.set_result(data)

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                transport.write(b"secure")  # type: ignore[attr-defined]
                self.transport = transport

        accept = asyncio.ensure_future(_accept(loop, listener))
        client_task = asyncio.ensure_future(
            loop.create_connection(Client, host, port, ssl=client_ctx, server_hostname="localhost")
        )
        conn = await accept
        server_transport, _ = await loop.connect_accepted_socket(Server, conn, ssl=server_ctx)
        result = await asyncio.wait_for(got, 5.0)
        client_transport, _ = await client_task
        server_transport.close()
        client_transport.close()
        listener.close()
        return result

    assert run(loop, main()) == b"secure"


def test_connect_accepted_socket_rejects_non_stream(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        with pytest.raises(ValueError, match="Stream Socket was expected"):
            await loop.connect_accepted_socket(asyncio.Protocol, sock)
        sock.close()

    run(loop, main())


def test_connect_accepted_socket_handshake_timeout_requires_ssl(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        sock = socket.socket()
        with pytest.raises(ValueError, match="ssl_handshake_timeout is only meaningful with ssl"):
            await loop.connect_accepted_socket(asyncio.Protocol, sock, ssl_handshake_timeout=1.0)
        sock.close()

    run(loop, main())


def test_connect_accepted_socket_shutdown_timeout_requires_ssl(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        sock = socket.socket()
        with pytest.raises(ValueError, match="ssl_shutdown_timeout is only meaningful with ssl"):
            await loop.connect_accepted_socket(asyncio.Protocol, sock, ssl_shutdown_timeout=1.0)
        sock.close()

    run(loop, main())


def test_connect_accepted_socket_closes_on_protocol_error(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        a, b = socket.socketpair()

        def protocol_factory() -> asyncio.Protocol:
            raise RuntimeError("boom")

        with pytest.raises(RuntimeError, match="boom"):
            await loop.connect_accepted_socket(protocol_factory, a)
        assert a.fileno() == -1  # the loop closed the adopted socket
        b.close()

    run(loop, main())


def test_connect_accepted_socket_tls_handshake_failure(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    _, cert = tls_certs
    server_ctx = _server_ctx(cert)

    async def main() -> None:
        listener = _listener()
        client = socket.socket()
        client.setblocking(False)
        try:
            client.connect(listener.getsockname())
        except BlockingIOError:
            pass
        conn = await _accept(loop, listener)
        # The client never sends a ClientHello; closing it aborts the handshake.
        client.close()
        with pytest.raises((ssl.SSLError, ConnectionError, OSError)):
            await asyncio.wait_for(
                loop.connect_accepted_socket(asyncio.Protocol, conn, ssl=server_ctx),
                5.0,
            )
        listener.close()

    run(loop, main())
