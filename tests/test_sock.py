from __future__ import annotations

import asyncio
import os
import socket
import struct
import tempfile

import pytest

from tests.conftest import run


def _tcp_listener() -> socket.socket:
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    sock.listen()
    sock.setblocking(False)
    return sock


def _pair() -> tuple[socket.socket, socket.socket]:
    a, b = socket.socketpair()
    a.setblocking(False)
    b.setblocking(False)
    return a, b


def _udp() -> tuple[socket.socket, socket.socket]:
    s1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s1.setblocking(False)
    s2.setblocking(False)
    s1.bind(("127.0.0.1", 0))
    return s1, s2


def test_sock_accept_connect_recv_sendall(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        listener = _tcp_listener()
        addr = listener.getsockname()

        async def server() -> None:
            conn, _ = await loop.sock_accept(listener)
            conn.setblocking(False)
            data = await loop.sock_recv(conn, 1024)
            await loop.sock_sendall(conn, data)
            conn.close()

        task = asyncio.ensure_future(server())
        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, addr)
        await loop.sock_sendall(client, b"ping")
        echoed = await loop.sock_recv(client, 1024)
        await task
        client.close()
        listener.close()
        return echoed

    assert run(loop, main()) == b"ping"


def test_sock_recv_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        a, b = _pair()
        recv = asyncio.ensure_future(loop.sock_recv(b, 1024))
        await asyncio.sleep(0.01)
        assert not recv.done()
        await loop.sock_sendall(a, b"late")
        data = await recv
        a.close()
        b.close()
        return data

    assert run(loop, main()) == b"late"


def test_sock_recv_into_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        a, b = _pair()
        buf = bytearray(8)
        recv = asyncio.ensure_future(loop.sock_recv_into(b, buf))
        await asyncio.sleep(0.01)
        await loop.sock_sendall(a, b"into")
        n = await recv
        a.close()
        b.close()
        return bytes(buf[:n])

    assert run(loop, main()) == b"into"


def test_sock_recvfrom_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        s1, s2 = _udp()
        recv = asyncio.ensure_future(loop.sock_recvfrom(s1, 1024))
        await asyncio.sleep(0.01)
        await loop.sock_sendto(s2, b"dgram", s1.getsockname())
        data, _ = await recv
        s1.close()
        s2.close()
        return data

    assert run(loop, main()) == b"dgram"


def test_sock_recvfrom_into_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        s1, s2 = _udp()
        buf = bytearray(16)
        recv = asyncio.ensure_future(loop.sock_recvfrom_into(s1, buf, 16))
        await asyncio.sleep(0.01)
        await loop.sock_sendto(s2, b"into-dg", s1.getsockname())
        n, _ = await recv
        s1.close()
        s2.close()
        return bytes(buf[:n])

    assert run(loop, main()) == b"into-dg"


def test_sock_recvfrom_into_default_nbytes(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        s1, s2 = _udp()
        await loop.sock_sendto(s2, b"hi", s1.getsockname())
        await asyncio.sleep(0.01)
        buf = bytearray(8)
        n, _ = await loop.sock_recvfrom_into(s1, buf)
        s1.close()
        s2.close()
        return bytes(buf[:n])

    assert run(loop, main()) == b"hi"


def test_sock_recv_error_propagates(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        listener = _tcp_listener()
        addr = listener.getsockname()
        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, addr)
        server_side, _ = await loop.sock_accept(listener)

        recv = asyncio.ensure_future(loop.sock_recv(client, 1024))
        await asyncio.sleep(0.01)
        # Send an RST so the pending recv becomes readable and then errors.
        server_side.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
        server_side.close()
        with pytest.raises(OSError):
            await recv
        client.close()
        listener.close()

    run(loop, main())


def test_sock_sendall_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> int:
        a, b = _pair()
        payload = b"x" * (1 << 20)

        async def drain() -> int:
            total = 0
            while total < len(payload):
                chunk = await loop.sock_recv(b, 65536)
                if not chunk:
                    break
                total += len(chunk)
            return total

        reader = asyncio.ensure_future(drain())
        await loop.sock_sendall(a, payload)
        a.close()
        received = await reader
        b.close()
        return received

    assert run(loop, main()) == (1 << 20)


def test_sock_sendall_error_propagates(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        a, b = _pair()
        b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        # A payload larger than the buffers forces sendall to defer; closing the
        # reader then makes the retried send raise.
        send = asyncio.ensure_future(loop.sock_sendall(a, b"y" * (4 << 20)))
        await asyncio.sleep(0.01)
        b.close()
        with pytest.raises(OSError):
            await send
        a.close()

    run(loop, main())


def test_sock_sendto_immediate(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> int:
        s1, s2 = _udp()
        n = await loop.sock_sendto(s2, b"quick", s1.getsockname())
        s1.close()
        s2.close()
        return n

    assert run(loop, main()) == 5


def test_sock_connect_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        listener = _tcp_listener()
        addr = listener.getsockname()

        async def server() -> None:
            conn, _ = await loop.sock_accept(listener)
            conn.setblocking(False)
            await loop.sock_sendall(conn, b"hi")
            conn.close()

        task = asyncio.ensure_future(server())
        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, addr)
        data = await loop.sock_recv(client, 1024)
        await task
        client.close()
        listener.close()
        return data

    assert run(loop, main()) == b"hi"


def test_sock_connect_refused(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        listener = _tcp_listener()
        addr = listener.getsockname()
        listener.close()
        client = socket.socket()
        client.setblocking(False)
        with pytest.raises(OSError):
            await loop.sock_connect(client, addr)
        client.close()

    run(loop, main())


def test_sock_connect_immediate_unix(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        # A non-AF_INET connect that completes synchronously skips both the
        # resolution branch and the writer registration.
        a, b = _pair()
        with pytest.raises(OSError):
            await loop.sock_connect(a, b"/nonexistent/zloop.sock")
        a.close()
        b.close()

    run(loop, main())


def test_sock_accept_deferred(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bool:
        listener = _tcp_listener()
        accept = asyncio.ensure_future(loop.sock_accept(listener))
        await asyncio.sleep(0.01)
        assert not accept.done()
        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, listener.getsockname())
        conn, _ = await accept
        conn.close()
        client.close()
        listener.close()
        return True

    assert run(loop, main()) is True


def test_sock_accept_cancelled_removes_reader(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        listener = _tcp_listener()
        accept = asyncio.ensure_future(loop.sock_accept(listener))
        await asyncio.sleep(0.01)
        accept.cancel()
        with pytest.raises(asyncio.CancelledError):
            await accept
        listener.close()

    run(loop, main())


def test_sock_op_rejected_when_fd_owned_by_transport(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()
        client = socket.socket()
        client.setblocking(False)
        await loop.sock_connect(client, addr)
        transport, _ = await loop.create_connection(asyncio.Protocol, sock=client)
        try:
            owned = transport.get_extra_info("socket")
            with pytest.raises(RuntimeError, match="is used by transport"):
                await loop.sock_recv(owned, 1)
        finally:
            transport.close()
            server.close()
            await server.wait_closed()

    run(loop, main())


def test_sock_sendall_first_send_blocks(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        a, b = _pair()
        a.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 1024)
        b.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        prefill = 0
        try:
            while True:
                prefill += a.send(b"f" * 4096)
        except BlockingIOError:
            pass

        marker = b"g" * 4096

        async def drain() -> bytes:
            total = 0
            tail = b""
            while total < prefill + len(marker):
                chunk = await loop.sock_recv(b, 65536)
                total += len(chunk)
                tail = chunk
            return tail

        reader = asyncio.ensure_future(drain())
        await loop.sock_sendall(a, marker)
        last = await reader
        a.close()
        b.close()
        return last[-4:]

    assert run(loop, main()) == b"gggg"


def test_sock_op_skips_unrelated_transport(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        server = await loop.create_server(asyncio.Protocol, "127.0.0.1", 0)
        addr = server.sockets[0].getsockname()
        held = socket.socket()
        held.setblocking(False)
        await loop.sock_connect(held, addr)
        transport, _ = await loop.create_connection(asyncio.Protocol, sock=held)
        try:
            # A deferred sock op on an unrelated fd must scan past the tracked
            # transport (the loop body's no-match continue branch).
            a, b = _pair()
            recv = asyncio.ensure_future(loop.sock_recv(b, 16))
            await asyncio.sleep(0.01)
            await loop.sock_sendall(a, b"ok")
            data = await recv
            a.close()
            b.close()
            return data
        finally:
            transport.close()
            server.close()
            await server.wait_closed()

    assert run(loop, main()) == b"ok"


def test_sock_connect_immediate(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> bytes:
        path = os.path.join(tempfile.mkdtemp(), "zloop.sock")
        listener = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        listener.bind(path)
        listener.listen()
        listener.setblocking(False)

        async def server() -> None:
            conn, _ = await loop.sock_accept(listener)
            conn.setblocking(False)
            await loop.sock_sendall(conn, b"unix")
            conn.close()

        task = asyncio.ensure_future(server())
        client = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        client.setblocking(False)
        await loop.sock_connect(client, path)
        data = await loop.sock_recv(client, 16)
        await task
        client.close()
        listener.close()
        os.unlink(path)
        return data

    assert run(loop, main()) == b"unix"


def test_sock_recv_debug_requires_non_blocking(loop: asyncio.AbstractEventLoop) -> None:
    async def main() -> None:
        loop.set_debug(True)
        a, b = socket.socketpair()
        with pytest.raises(ValueError, match="non-blocking"):
            await loop.sock_recv(a, 1)
        a.close()
        b.close()

    run(loop, main())
