"""TLS support by reusing asyncio's loop-agnostic SSL state machine.

asyncio.sslproto.SSLProtocol implements the full TLS handshake and record
framing in pure Python; it only needs a loop with call_soon/call_later and a
plaintext-side transport. We wrap our Zig socket transport with it: the raw
transport ferries ciphertext to/from the socket, SSLProtocol decrypts, and the
application protocol talks to the SSL protocol's app-facing transport.
"""

from __future__ import annotations

import asyncio
from asyncio import sslproto
from typing import Any


def start_tls_transport(
    loop: Any,
    sock: Any,
    app_protocol: Any,
    sslcontext: Any,
    extra: dict[str, Any],
    *,
    server_side: bool,
    server_hostname: str | None = None,
    ssl_handshake_timeout: float | None = None,
    ssl_shutdown_timeout: float | None = None,
) -> Any:
    waiter: asyncio.Future[None] = loop.create_future()
    ssl_protocol = sslproto.SSLProtocol(
        loop,
        app_protocol,
        sslcontext,
        waiter,
        server_side,
        server_hostname,
        ssl_handshake_timeout=ssl_handshake_timeout,
        ssl_shutdown_timeout=ssl_shutdown_timeout,
        call_connection_made=True,
    )
    # The raw transport drives ciphertext; SSLProtocol is its protocol. Creating
    # the transport delivers ssl_protocol.connection_made(raw) and begins
    # reading, kicking off the handshake.
    loop._make_transport(sock.fileno(), ssl_protocol, extra)

    # Surface handshake failures rather than leaving the waiter pending. We do
    # not block here: the app protocol's connection_made fires after a
    # successful handshake; on failure the waiter's exception is logged.
    def _check(fut: asyncio.Future[None]) -> None:
        if fut.cancelled():
            return
        exc = fut.exception()
        if exc is not None:
            loop.call_exception_handler(
                {"message": "TLS handshake failed", "exception": exc}
            )

    waiter.add_done_callback(_check)
    return ssl_protocol._get_app_transport()
