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


def test_tls_roundtrip(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    ca, cert = tls_certs
    server_ctx = _server_ctx(cert)
    client_ctx = _client_ctx(ca)

    class Echo(asyncio.Protocol):
        def connection_made(self, transport: asyncio.BaseTransport) -> None:
            self.transport = transport

        def data_received(self, data: bytes) -> None:
            self.transport.write(b"secure:" + data)  # type: ignore[attr-defined]

    async def main() -> bytes:
        server = await loop.create_server(Echo, "127.0.0.1", 0, ssl=server_ctx)
        host, port = server.sockets[0].getsockname()
        got: asyncio.Future[bytes] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                transport.write(b"hi")  # type: ignore[attr-defined]
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                if not got.done():
                    got.set_result(data)
                self.transport.close()  # type: ignore[attr-defined]

        await loop.create_connection(Client, host, port, ssl=client_ctx, server_hostname="localhost")
        result = await asyncio.wait_for(got, 5.0)
        server.close()
        await server.wait_closed()
        return result

    assert run(loop, main()) == b"secure:hi"


def test_tls_extra_info_sslcontext(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    ca, cert = tls_certs
    server_ctx = _server_ctx(cert)
    client_ctx = _client_ctx(ca)
    seen: dict[str, object] = {}

    class Server(asyncio.Protocol):
        def connection_made(self, transport: asyncio.Transport) -> None:
            seen["ssl_object"] = transport.get_extra_info("ssl_object")
            self.transport = transport
            transport.write(b"ok")

    async def main() -> None:
        server = await loop.create_server(Server, "127.0.0.1", 0, ssl=server_ctx)
        host, port = server.sockets[0].getsockname()
        done: asyncio.Future[None] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                if not done.done():
                    done.set_result(None)
                self.transport.close()  # type: ignore[attr-defined]

        await loop.create_connection(Client, host, port, ssl=client_ctx, server_hostname="localhost")
        await asyncio.wait_for(done, 5.0)
        server.close()
        await server.wait_closed()

    run(loop, main())
    assert seen["ssl_object"] is not None


def test_tls_server_hostname_defaults_to_host(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    """When server_hostname is omitted, create_connection derives it from host."""
    ca, cert = tls_certs
    server_ctx = _server_ctx(cert)
    client_ctx = _client_ctx(ca)

    class Server(asyncio.Protocol):
        def connection_made(self, transport: asyncio.Transport) -> None:
            transport.write(b"ok")
            self.transport = transport

    async def main() -> None:
        server = await loop.create_server(Server, "127.0.0.1", 0, ssl=server_ctx)
        _, port = server.sockets[0].getsockname()
        done: asyncio.Future[None] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                if not done.done():
                    done.set_result(None)
                self.transport.close()  # type: ignore[attr-defined]

        # host is "localhost" so server_hostname is derived from it
        await loop.create_connection(Client, "localhost", port, ssl=client_ctx)
        await asyncio.wait_for(done, 5.0)
        server.close()
        await server.wait_closed()

    run(loop, main())


def test_tls_server_handshake_failure_logged(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    """A client that aborts the TLS handshake triggers the server's failure log."""
    ca, cert = tls_certs
    server_ctx = _server_ctx(cert)
    seen: list[dict] = []

    async def main() -> None:
        loop.set_exception_handler(lambda lp, ctx: seen.append(ctx))
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, ssl=server_ctx)
        host, port = server.sockets[0].getsockname()
        # connect a plain (non-TLS) client and send garbage so the server's
        # handshake fails.
        s = socket.create_connection((host, port))
        s.sendall(b"not a tls handshake\r\n\r\n")
        s.close()
        await asyncio.sleep(0.3)
        server.close()
        await server.wait_closed()

    run(loop, main())
    assert any("TLS handshake failed" in str(ctx.get("message")) for ctx in seen)


def test_tls_handshake_failure_is_handled(loop: asyncio.AbstractEventLoop, tls_certs) -> None:
    """A client with no trust should fail the handshake without hanging the loop."""
    ca, cert = tls_certs
    server_ctx = _server_ctx(cert)
    # client context that does not trust the server's CA
    bad_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0, ssl=server_ctx)
        host, port = server.sockets[0].getsockname()
        with pytest.raises((ssl.SSLError, ssl.SSLCertVerificationError, ConnectionError, OSError)):
            await asyncio.wait_for(
                loop.create_connection(asyncio.Protocol, host, port, ssl=bad_ctx, server_hostname="localhost"),
                5.0,
            )
        server.close()
        await server.wait_closed()

    run(loop, main())
