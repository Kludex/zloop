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
import functools
import signal as signal_module
import socket
import ssl as ssl_module
import threading
from asyncio import base_events
from typing import Any

from zloop import _zloop
from zloop._tls import start_tls_transport


class _SignalHandle:
    """A minimal Handle for a registered signal callback."""

    __slots__ = ("_callback", "_cancelled")

    def __init__(self, callback: Any) -> None:
        self._callback = callback
        self._cancelled = False

    def cancelled(self) -> bool:
        return self._cancelled

    def cancel(self) -> None:  # pragma: no cover - parity with asyncio.Handle
        self._cancelled = True

    def _run(self) -> None:
        self._callback()


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
    except OSError:  # pragma: no cover - listening sockets have no peer
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

    @property
    def sockets(self) -> tuple[socket.socket, ...]:
        if self._sockets is None:
            return ()
        return tuple(self._sockets)

    def is_serving(self) -> bool:
        return self._active

    def get_loop(self) -> Loop:
        return self._loop

    def _start_serving(self) -> None:
        if self._active:
            return
        self._active = True
        for sock in self._sockets or ():
            sock.listen(self._backlog)
            sock.setblocking(False)
            self._loop.add_reader(sock.fileno(), self._accept, sock)

    def _accept(self, sock: socket.socket) -> None:
        # Drain all pending connections for this readiness, stopping on EWOULDBLOCK.
        while True:
            try:
                conn, _addr = sock.accept()
            except (BlockingIOError, InterruptedError):
                return
            except OSError as exc:  # pragma: no cover - resource exhaustion path
                if exc.errno in (errno.EMFILE, errno.ENFILE, errno.ENOBUFS, errno.ENOMEM):
                    self._loop.call_exception_handler(
                        {"message": "socket.accept() out of system resource", "exception": exc, "socket": sock}
                    )
                    return
                raise
            self._loop._connect_accepted(conn, self._protocol_factory, self._ssl)

    def close(self) -> None:
        sockets = self._sockets
        if sockets is None:
            return
        self._sockets = None
        for sock in sockets:
            self._loop.remove_reader(sock.fileno())
            sock.close()
        self._active = False
        for waiter in self._waiters:
            if not waiter.done():  # pragma: no branch - waiters are pending until now
                waiter.set_result(None)
        self._waiters.clear()

    def close_clients(self) -> None:  # pragma: no cover - parity with asyncio
        pass

    def abort_clients(self) -> None:  # pragma: no cover - parity with asyncio
        pass

    async def start_serving(self) -> None:
        self._start_serving()

    async def serve_forever(self) -> None:  # pragma: no cover - not used by uvicorn
        self._start_serving()
        future: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(future)
        await future

    async def wait_closed(self) -> None:
        if self._sockets is None:
            return
        waiter: asyncio.Future[None] = self._loop.create_future()
        self._waiters.append(waiter)
        await waiter

    async def __aenter__(self) -> _Server:  # pragma: no cover - parity
        return self

    async def __aexit__(self, *exc: object) -> None:  # pragma: no cover - parity
        self.close()
        await self.wait_closed()


class Loop(_zloop.Loop):
    """The public zloop event loop: the Zig engine plus connection setup."""

    _default_executor: concurrent.futures.Executor | None = None
    _executor_shutdown_called: bool = False
    _transports: set[Any]

    # -- transport lifetime ---------------------------------------------------
    #
    # asyncio keeps accepted/connected transports alive until connection_lost
    # even if the protocol does not retain them. The Zig transport calls these
    # back; the set is a GC root reachable from the running loop.

    def _track_transport(self, transport: Any) -> None:
        try:
            self._transports.add(transport)
        except AttributeError:
            self._transports = {transport}

    def _untrack_transport(self, transport: Any) -> None:
        # _untrack only follows a _track, so the set always exists here.
        self._transports.discard(transport)

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
        thread = threading.Thread(target=self._do_shutdown_executor, args=(future,))
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
        except Exception as exc:
            self.call_soon_threadsafe(future.set_exception, exc)

    # -- async generators -----------------------------------------------------

    async def shutdown_asyncgens(self) -> None:
        # zloop does not register asyncgen finalizer hooks; nothing tracked.
        return None

    # -- signals --------------------------------------------------------------
    #
    # Implemented like asyncio's selector loop: a self-pipe receives the signal
    # numbers written by signal.set_wakeup_fd; a reader on it dispatches the
    # registered handlers on the loop thread.

    _signal_handlers: dict[int, Any]
    _signal_rsock: socket.socket | None = None
    _signal_wsock: socket.socket | None = None

    def _ensure_signal_pipe(self) -> None:
        if self._signal_rsock is not None:
            return
        if threading.current_thread() is not threading.main_thread():
            raise ValueError("add_signal_handler() can only be called from the main thread")
        self._signal_handlers = {}
        rsock, wsock = socket.socketpair()
        rsock.setblocking(False)
        wsock.setblocking(False)
        self._signal_rsock = rsock
        self._signal_wsock = wsock
        signal_module.set_wakeup_fd(wsock.fileno())
        self.add_reader(rsock.fileno(), self._process_signals)

    def _process_signals(self) -> None:
        assert self._signal_rsock is not None
        try:
            data = self._signal_rsock.recv(4096)
        except (BlockingIOError, InterruptedError):  # pragma: no cover - spurious wakeup
            return
        for signum in data:
            handle = self._signal_handlers.get(signum)
            if handle is not None and not handle.cancelled():  # pragma: no branch
                handle._run()

    def add_signal_handler(self, sig: int, callback: Any, *args: Any) -> None:
        if not isinstance(sig, int):  # pragma: no cover - parity with asyncio
            raise TypeError("sig must be an int")
        self._ensure_signal_pipe()
        # Install a no-op C handler; the wakeup fd carries the signum to us.
        try:
            signal_module.signal(sig, lambda s, f: None)
        except (ValueError, OSError) as exc:  # pragma: no cover - bad signal
            raise RuntimeError(str(exc)) from None
        signal_module.siginterrupt(sig, False)
        self._signal_handlers[sig] = _SignalHandle(functools.partial(callback, *args))

    def remove_signal_handler(self, sig: int) -> bool:
        handlers = getattr(self, "_signal_handlers", None)
        if not handlers or sig not in handlers:
            return False
        del handlers[sig]
        try:
            signal_module.signal(sig, signal_module.SIG_DFL)
        except (ValueError, OSError):  # pragma: no cover
            return False
        if not handlers and self._signal_rsock is not None:
            signal_module.set_wakeup_fd(-1)
            self.remove_reader(self._signal_rsock.fileno())
            self._signal_rsock.close()
            self._signal_wsock.close()  # type: ignore[union-attr]
            self._signal_rsock = None
            self._signal_wsock = None
        return True

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
        return await self.run_in_executor(None, socket.getaddrinfo, host, port, family, type, proto, flags)

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
                    msg = f"error while attempting to bind on address {sa!r}: {exc.strerror}"
                    raise OSError(exc.errno, msg) from None
                sockets.append(sock)
            completed = True
        finally:
            if not completed:  # pragma: no cover - partial multi-address bind cleanup
                for s in sockets:
                    s.close()
        return sockets

    def _connect_accepted(self, conn: socket.socket, protocol_factory: Any, ssl: Any) -> None:
        conn.setblocking(False)
        # Own the socket until the transport adopts its fd; close it if protocol
        # construction or transport setup raises, so accepted fds never leak.
        adopted = False
        try:
            protocol = protocol_factory()
            extra = _make_extra(conn, ssl if ssl else None)
            if ssl:
                _, waiter = start_tls_transport(self, conn, protocol, ssl, extra, server_side=True)

                def _log_failure(fut: asyncio.Future[None]) -> None:
                    if not fut.cancelled() and fut.exception() is not None:
                        self.call_exception_handler({"message": "TLS handshake failed", "exception": fut.exception()})

                waiter.add_done_callback(_log_failure)
            else:
                self._make_transport(conn.fileno(), protocol, extra)
            adopted = True
        finally:
            if not adopted:
                conn.close()

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
                    if local_addr is not None:
                        sock.bind(local_addr)
                    fut: asyncio.Future[None] = self.create_future()
                    self._sock_connect_start(fut, sock, address)
                    await fut
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

        # Own the connected socket until the transport adopts it; close it on any
        # failure during protocol construction / transport setup so it can't leak.
        adopted = False
        try:
            protocol = protocol_factory()
            extra = _make_extra(sock, ssl if ssl else None)
            if ssl:
                transport, waiter = start_tls_transport(
                    self,
                    sock,
                    protocol,
                    ssl,
                    extra,
                    server_side=False,
                    server_hostname=server_hostname,
                    ssl_handshake_timeout=ssl_handshake_timeout,
                )
                adopted = True
                try:
                    await waiter
                except BaseException:
                    transport.close()
                    raise
            else:
                transport = self._make_transport(sock.fileno(), protocol, extra)
                adopted = True
        finally:
            if not adopted:
                sock.close()
        return transport, protocol

    # -- low-level socket coroutines (sock_*) ---------------------------------
    #
    # Mirrors asyncio's BaseSelectorEventLoop, adapted to the engine's readiness
    # primitives (add_reader/add_writer take *args but return no Handle, and
    # allow one reader and one writer per fd). Each method tries the syscall
    # once, then registers a callback that retries on readiness; a future
    # done-callback removes the registration on completion or cancellation.

    def _ensure_fd_no_transport(self, sock: socket.socket) -> None:
        fd = sock.fileno()
        for transport in getattr(self, "_transports", ()):
            other = transport.get_extra_info("socket")
            if other is not None and other.fileno() == fd and not transport.is_closing():
                raise RuntimeError(f"File descriptor {fd!r} is used by transport {transport!r}")

    def _check_sock(self, sock: socket.socket) -> None:
        base_events._check_ssl_socket(sock)  # type: ignore[attr-defined]
        if self.get_debug() and sock.gettimeout() != 0:
            raise ValueError("the socket must be non-blocking")

    async def sock_recv(self, sock: socket.socket, n: int) -> bytes:
        self._check_sock(sock)
        try:
            return sock.recv(n)
        except (BlockingIOError, InterruptedError):
            pass
        return await self._sock_read(sock, lambda: sock.recv(n))

    async def sock_recv_into(self, sock: socket.socket, buf: Any) -> int:
        self._check_sock(sock)
        try:
            return sock.recv_into(buf)
        except (BlockingIOError, InterruptedError):
            pass
        return await self._sock_read(sock, lambda: sock.recv_into(buf))

    async def sock_recvfrom(self, sock: socket.socket, bufsize: int) -> Any:
        self._check_sock(sock)
        try:
            return sock.recvfrom(bufsize)
        except (BlockingIOError, InterruptedError):
            pass
        return await self._sock_read(sock, lambda: sock.recvfrom(bufsize))

    async def sock_recvfrom_into(self, sock: socket.socket, buf: Any, nbytes: int = 0) -> Any:
        self._check_sock(sock)
        if not nbytes:
            nbytes = len(buf)
        try:
            return sock.recvfrom_into(buf, nbytes)
        except (BlockingIOError, InterruptedError):
            pass
        return await self._sock_read(sock, lambda: sock.recvfrom_into(buf, nbytes))

    async def _sock_read(self, sock: socket.socket, attempt: Any) -> Any:
        self._ensure_fd_no_transport(sock)
        fut: asyncio.Future[Any] = self.create_future()
        fd = sock.fileno()
        self.add_reader(fd, self._sock_retry, fut, attempt)
        fut.add_done_callback(functools.partial(self._sock_read_done, fd))
        return await fut

    def _sock_read_done(self, fd: int, fut: asyncio.Future[Any]) -> None:
        self.remove_reader(fd)

    def _sock_write_done(self, fd: int, fut: asyncio.Future[Any]) -> None:
        self.remove_writer(fd)

    def _sock_retry(self, fut: asyncio.Future[Any], attempt: Any) -> None:
        if fut.done():
            return
        try:
            result = attempt()
        except (BlockingIOError, InterruptedError):  # pragma: no cover - spurious readiness wakeup
            return
        except (SystemExit, KeyboardInterrupt):  # pragma: no cover - parity with asyncio
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(result)

    async def sock_sendall(self, sock: socket.socket, data: Any) -> None:
        self._check_sock(sock)
        try:
            n = sock.send(data)
        except (BlockingIOError, InterruptedError):
            n = 0
        if n == len(data):
            return
        self._ensure_fd_no_transport(sock)
        fut: asyncio.Future[None] = self.create_future()
        fd = sock.fileno()
        self.add_writer(fd, self._sock_sendall, fut, sock, memoryview(data), [n])
        fut.add_done_callback(functools.partial(self._sock_write_done, fd))
        return await fut

    def _sock_sendall(self, fut: asyncio.Future[None], sock: socket.socket, view: memoryview, pos: list[int]) -> None:
        if fut.done():
            return
        start = pos[0]
        try:
            n = sock.send(view[start:])
        except (BlockingIOError, InterruptedError):  # pragma: no cover - spurious writability wakeup
            return
        except (SystemExit, KeyboardInterrupt):  # pragma: no cover - parity with asyncio
            raise
        except BaseException as exc:
            fut.set_exception(exc)
            return
        start += n
        if start == len(view):
            fut.set_result(None)
        else:  # pragma: no cover - kernel buffer drained in one pass under test
            pos[0] = start

    async def sock_sendto(self, sock: socket.socket, data: Any, address: Any) -> int:
        self._check_sock(sock)
        try:
            return sock.sendto(data, address)
        except (BlockingIOError, InterruptedError):  # pragma: no cover - datagram sends rarely block on loopback
            return await self._sock_sendto_deferred(sock, data, address)

    async def _sock_sendto_deferred(  # pragma: no cover - datagram sends rarely block on loopback
        self, sock: socket.socket, data: Any, address: Any
    ) -> int:
        self._ensure_fd_no_transport(sock)
        fut: asyncio.Future[int] = self.create_future()
        fd = sock.fileno()
        self.add_writer(fd, self._sock_sendto, fut, sock, data, address)
        fut.add_done_callback(functools.partial(self._sock_write_done, fd))
        return await fut

    def _sock_sendto(  # pragma: no cover - datagram sends rarely block on loopback
        self, fut: asyncio.Future[int], sock: socket.socket, data: Any, address: Any
    ) -> None:
        if fut.done():
            return
        try:
            n = sock.sendto(data, 0, address)
        except (BlockingIOError, InterruptedError):
            return
        except (SystemExit, KeyboardInterrupt):
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(n)

    async def sock_connect(self, sock: socket.socket, address: Any) -> None:
        self._check_sock(sock)
        if sock.family in (socket.AF_INET, socket.AF_INET6):
            infos = await self.getaddrinfo(*address[:2], family=sock.family, type=sock.type, proto=sock.proto)
            _, _, _, _, address = infos[0]
        fut: asyncio.Future[None] = self.create_future()
        self._sock_connect_start(fut, sock, address)
        return await fut

    def _sock_connect_start(self, fut: asyncio.Future[None], sock: socket.socket, address: Any) -> None:
        fd = sock.fileno()
        try:
            sock.connect(address)
        except (BlockingIOError, InterruptedError):
            self._ensure_fd_no_transport(sock)
            self.add_writer(fd, self._sock_connect_cb, fut, sock, address)
            fut.add_done_callback(functools.partial(self._sock_write_done, fd))
        except (SystemExit, KeyboardInterrupt):  # pragma: no cover - parity with asyncio
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(None)

    def _sock_connect_cb(self, fut: asyncio.Future[None], sock: socket.socket, address: Any) -> None:
        if fut.done():  # pragma: no cover - cancellation races the writer
            return
        try:
            err = sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
            if err != 0:
                raise OSError(err, f"Connect call failed {address}")
        except (BlockingIOError, InterruptedError):  # pragma: no cover - retried on next readiness
            pass
        except (SystemExit, KeyboardInterrupt):  # pragma: no cover - parity with asyncio
            raise
        except BaseException as exc:
            fut.set_exception(exc)
        else:
            fut.set_result(None)

    async def sock_accept(self, sock: socket.socket) -> Any:
        self._check_sock(sock)
        fut: asyncio.Future[Any] = self.create_future()
        self._sock_accept(fut, sock)
        return await fut

    def _sock_accept(self, fut: asyncio.Future[Any], sock: socket.socket) -> None:
        fd = sock.fileno()
        try:
            conn, address = sock.accept()
            conn.setblocking(False)
        except (BlockingIOError, InterruptedError):
            self._ensure_fd_no_transport(sock)
            self.add_reader(fd, self._sock_accept, fut, sock)
            fut.add_done_callback(functools.partial(self._sock_read_done, fd))
        except (SystemExit, KeyboardInterrupt):  # pragma: no cover - parity with asyncio
            raise
        except BaseException as exc:  # pragma: no cover - accept errors are environment-specific
            fut.set_exception(exc)
        else:
            fut.set_result((conn, address))
