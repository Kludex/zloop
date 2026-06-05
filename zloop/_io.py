"""High-level connection setup, layered on the Zig engine.

The event loop, timers, I/O readiness, and the socket transport all live in the
Zig core (`_zloop`). This module only contains the one-time *choreography* of
turning a host/port (or socket) into a listening or connected transport:
address resolution, socket creation, bind/listen, the accept loop, and the
async connect handshake. It calls down into the engine primitives
(`add_reader`, `_make_transport`, `run_in_executor`) - it never moves bytes.
"""

from __future__ import annotations

import asyncio
import collections.abc
import concurrent.futures
import errno
import socket
import ssl as ssl_module
from typing import Any

from zloop import _zloop


def _make_extra(sock: socket.socket, sslcontext: ssl_module.SSLContext | None = None) -> dict[str, Any]:
    # Keep the socket object in `extra` so the transport (which holds extra)
    # keeps it alive; the transport owns the fd via the socket object's lifetime.
    extra: dict[str, Any] = {"socket": sock}
    try:
        extra["sockname"] = sock.getsockname()
    except OSError:  # pragma: no cover
        pass
    try:
        extra["peername"] = sock.getpeername()
    except OSError:
        pass
    if sslcontext is not None:
        extra["sslcontext"] = sslcontext
    return extra


class _Server(asyncio.AbstractServer):
    """An asyncio-compatible Server bound to one or more listening sockets."""

    def __init__(self, loop: Loop, sockets: list[socket.socket], protocol_factory: Any, ssl: Any, backlog: int):
        self._loop = loop
        self._sockets: list[socket.socket] | None = sockets
        self._protocol_factory = protocol_factory
        self._ssl = ssl
        self._backlog = backlog
        self._active = False
        self._waiters: list[asyncio.Future[None]] = []
        self._open_count = 0

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        if self._sockets is None:
            return ()
        return tuple(self._sockets)

    def is_serving(self) -> bool:
        return self._active

    def _start_serving(self) -> None:
        if self._active:
            return
        self._active = True
        for sock in self._sockets or ():
            sock.listen(self._backlog)
            sock.setblocking(False)
            self._loop.add_reader(sock.fileno(), self._accept, sock)

    def _accept(self, sock: socket.socket) -> None:
        for _ in range(64):  # accept a bounded burst per readiness
            try:
                conn, addr = sock.accept()
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:  # pragma: no cover
                if exc.errno in (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM):
                    self._loop.call_exception_handler(
                        {"message": "socket.accept() out of system resource", "exception": exc, "socket": sock}
                    )
                    return
                raise
            self._loop._connect_accepted(conn, self._protocol_factory, self._ssl, self)

    def close(self) -> None:
        sockets = self._sockets
        if sockets is None:
            return
        self._sockets = None
        for sock in sockets:
            self._loop.remove_reader(sock.fileno())
            sock.close()
        self._active = False
        if self._open_count == 0:
            self._wakeup()

    def close_clients(self) -> None:  # pragma: no cover - parity with asyncio
        pass

    def abort_clients(self) -> None:  # pragma: no cover - parity with asyncio
        pass

    def _attach(self) -> None:
        self._open_count += 1

    def _detach(self) -> None:
        self._open_count -= 1
        if self._open_count == 0 and self._sockets is None:
            self._wakeup()

    def _wakeup(self) -> None:
        for waiter in self._waiters:
            if not waiter.done():
                waiter.set_result(None)
        self._waiters.clear()

    async def start_serving(self) -> None:
        self._start_serving()

    async def serve_forever(self) -> None:  # pragma: no cover - not used by uvicorn
        self._start_serving()
        future: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(future)
        await future

    async def wait_closed(self) -> None:
        if self._sockets is None and self._open_count == 0:
            return
        waiter: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

    async def __aenter__(self) -> _Server:  # pragma: no cover - parity
        return self

    async def __aexit__(self, *exc: object) -> None:  # pragma: no cover - parity
        self.close()
        await self.wait_closed()


class Loop(_zloop.Loop):  # type: ignore[misc,valid-type]
    """The public zloop event loop: the Zig engine plus connection setup."""

    _default_executor: concurrent.futures.Executor | None = None
    _executor_shutdown_called: bool = False

    # -- executors ------------------------------------------------------------

    def run_in_executor(self, executor: Any, func: Any, *args: Any) -> asyncio.Future[Any]:
        if executor is None:
            executor = self._default_executor
            if executor is None:
                executor = concurrent.futures.ThreadPoolExecutor(thread_name_prefix="zloop")
                self._default_executor = executor
        cf = executor.submit(func, *args)
        return asyncio.futures.wrap_future(cf, loop=self)

    def set_default_executor(self, executor: concurrent.futures.Executor) -> None:
        self._default_executor = executor

    async def shutdown_default_executor(self, timeout: float | None = None) -> None:
        self._executor_shutdown_called = True
        if self._default_executor is None:
            return
        future: asyncio.Future[None] = self.create_future()
        thread = __import__("threading").Thread(target=self._do_shutdown_executor, args=(future,))
        thread.start()
        try:
            await future
        finally:
            thread.join(timeout)

    def _do_shutdown_executor(self, future: asyncio.Future[None]) -> None:
        try:
            assert self._default_executor is not None
            self._default_executor.shutdown(wait=True)
            self.call_soon_threadsafe(future.set_result, None)
        except Exception as exc:  # noqa: BLE001 - executor threads may raise anything
            self.call_soon_threadsafe(future.set_exception, exc)

    # -- async generators -----------------------------------------------------

    async def shutdown_asyncgens(self) -> None:
        # zloop does not register asyncgen finalizer hooks; nothing tracked.
        return None

    async def getaddrinfo(
        self,
        host: str | None,
        port: str | int | None,
        *,
        family: int = 0,
        type: int = 0,
        proto: int = 0,
        flags: int = 0,
    ) -> list[Any]:
        return await self.run_in_executor(
            None, socket.getaddrinfo, host, port, family, type, proto, flags
        )

    async def getnameinfo(self, sockaddr: Any, flags: int = 0) -> Any:
        return await self.run_in_executor(None, socket.getnameinfo, sockaddr, flags)

    async def create_server(
        self,
        protocol_factory: Any,
        host: Any = None,
        port: Any = None,
        *,
        family: int = socket.AF_UNSPEC,
        flags: int = socket.AI_PASSIVE,
        sock: socket.socket | None = None,
        backlog: int = 100,
        ssl: Any = None,
        reuse_address: bool | None = None,
        reuse_port: bool | None = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
        start_serving: bool = True,
        **_: Any,
    ) -> asyncio.AbstractServer:
        if sock is not None:
            sockets = [sock]
        else:
            sockets = await self._create_listeners(host, port, family, flags, reuse_address, reuse_port)

        for s in sockets:
            s.setblocking(False)

        server = _Server(self, sockets, protocol_factory, ssl, backlog)
        if start_serving:
            server._start_serving()
            await asyncio.sleep(0)
        return server

    async def create_unix_server(
        self,
        protocol_factory: Any,
        path: Any = None,
        *,
        sock: socket.socket | None = None,
        backlog: int = 100,
        ssl: Any = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
        start_serving: bool = True,
        **_: Any,
    ) -> asyncio.AbstractServer:
        if sock is None:
            if path is None:
                raise ValueError("path was not specified, and no sock specified")
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            try:
                sock.bind(str(path))
            except OSError as exc:
                sock.close()
                raise OSError(exc.errno, f"error while attempting to bind on address {path!r}") from None
        sock.setblocking(False)
        server = _Server(self, [sock], protocol_factory, ssl, backlog)
        if start_serving:
            server._start_serving()
            await asyncio.sleep(0)
        return server

    async def _create_listeners(
        self,
        host: Any,
        port: Any,
        family: int,
        flags: int,
        reuse_address: bool | None,
        reuse_port: bool | None,
    ) -> list[socket.socket]:
        if reuse_address is None:
            reuse_address = True  # POSIX default for servers, matching asyncio

        hosts: list[Any]
        if host is None or host == "":
            hosts = [None]
        elif isinstance(host, str):
            hosts = [host]
        elif isinstance(host, collections.abc.Iterable):
            hosts = list(host)
        else:  # pragma: no cover
            hosts = [host]

        infos: list[Any] = []
        for h in hosts:
            results = await self.getaddrinfo(h, port, family=family, type=socket.SOCK_STREAM, flags=flags)
            infos.extend(results)

        sockets: list[socket.socket] = []
        completed = False
        try:
            for af, socktype, proto, _canon, sa in infos:
                try:
                    sock = socket.socket(af, socktype, proto)
                except OSError:  # pragma: no cover
                    continue
                if reuse_address:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                if reuse_port:  # pragma: no cover - rarely exercised
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
                if af == socket.AF_INET6 and hasattr(socket, "IPPROTO_IPV6"):
                    sock.setsockopt(socket.IPPROTO_IPV6, socket.IPV6_V6ONLY, True)
                try:
                    sock.bind(sa)
                except OSError as exc:
                    sock.close()
                    raise OSError(exc.errno, f"error while attempting to bind on address {sa!r}: {exc.strerror}") from None
                sockets.append(sock)
            completed = True
        finally:
            if not completed:
                for s in sockets:
                    s.close()
        return sockets

    def _connect_accepted(self, conn: socket.socket, protocol_factory: Any, ssl: Any, server: _Server) -> None:
        conn.setblocking(False)
        protocol = protocol_factory()
        extra = _make_extra(conn, ssl if ssl else None)
        if ssl:
            from zloop._tls import start_tls_transport

            start_tls_transport(self, conn, protocol, ssl, extra, server_side=True)
        else:
            self._make_transport(conn.fileno(), protocol, extra)

    async def create_connection(
        self,
        protocol_factory: Any,
        host: Any = None,
        port: Any = None,
        *,
        ssl: Any = None,
        family: int = 0,
        proto: int = 0,
        flags: int = 0,
        sock: socket.socket | None = None,
        local_addr: Any = None,
        server_hostname: Any = None,
        ssl_handshake_timeout: float | None = None,
        ssl_shutdown_timeout: float | None = None,
        happy_eyeballs_delay: float | None = None,
        interleave: int | None = None,
        all_errors: bool = False,
        **_: Any,
    ) -> tuple[Any, Any]:
        if sock is None:
            infos = await self.getaddrinfo(host, port, family=family, type=socket.SOCK_STREAM, proto=proto, flags=flags)
            exceptions: list[OSError] = []
            for af, socktype, pr, _canon, address in infos:
                sock = socket.socket(af, socktype, pr)
                sock.setblocking(False)
                try:
                    await self._sock_connect(sock, address)
                    break
                except OSError as exc:
                    sock.close()
                    sock = None
                    exceptions.append(exc)
            if sock is None:
                raise exceptions[-1] if exceptions else OSError("getaddrinfo returned empty list")
        else:
            sock.setblocking(False)

        if server_hostname is None and ssl and host:
            server_hostname = host

        protocol = protocol_factory()
        extra = _make_extra(sock, ssl if ssl else None)
        if ssl:
            from zloop._tls import start_tls_transport

            transport = start_tls_transport(
                self, sock, protocol, ssl, extra, server_side=False, server_hostname=server_hostname
            )
        else:
            transport = self._make_transport(sock.fileno(), protocol, extra)
        return transport, protocol

    async def _sock_connect(self, sock: socket.socket, address: Any) -> None:
        try:
            sock.connect(address)
            return
        except BlockingIOError:
            pass
        future: asyncio.Future[None] = self.create_future()

        def _on_writable() -> None:
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                if not future.done():
                    future.set_exception(OSError(err, f"Connect call failed {address!r}"))
            elif not future.done():
                future.set_result(None)

        self.add_writer(sock.fileno(), _on_writable)
        try:
            await future
        finally:
            self.remove_writer(sock.fileno())
