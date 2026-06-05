from __future__ import annotations

import asyncio

import pytest

from tests.conftest import run


def test_default_exception_handler_prints(loop: asyncio.AbstractEventLoop, capsys) -> None:
    loop.call_exception_handler({"message": "boom"})
    captured = capsys.readouterr()
    assert "boom" in captured.err


def test_default_exception_handler_with_exception(loop: asyncio.AbstractEventLoop, capsys) -> None:
    try:
        raise ValueError("kaboom")
    except ValueError as exc:
        loop.call_exception_handler({"message": "failed", "exception": exc, "future": "x"})
    captured = capsys.readouterr()
    assert "failed" in captured.err
    assert "kaboom" in captured.err
    assert "future" in captured.err


def test_default_exception_handler_no_message(loop: asyncio.AbstractEventLoop, capsys) -> None:
    loop.call_exception_handler({})
    captured = capsys.readouterr()
    assert "Unhandled exception in event loop" in captured.err


def test_custom_exception_handler(loop: asyncio.AbstractEventLoop) -> None:
    seen: list[dict] = []
    loop.set_exception_handler(lambda lp, ctx: seen.append(ctx))
    assert loop.get_exception_handler() is not None
    loop.call_exception_handler({"message": "x"})
    assert seen == [{"message": "x"}]


def test_custom_exception_handler_must_be_callable(loop: asyncio.AbstractEventLoop) -> None:
    with pytest.raises(TypeError):
        loop.set_exception_handler(123)  # type: ignore[arg-type]


def test_custom_exception_handler_falls_back_on_error(loop: asyncio.AbstractEventLoop, capsys) -> None:
    def bad_handler(lp, ctx):
        raise RuntimeError("handler itself failed")

    loop.set_exception_handler(bad_handler)
    loop.call_exception_handler({"message": "original"})
    captured = capsys.readouterr()
    # falls back to default handler which prints
    assert "original" in captured.err


def test_set_exception_handler_none_resets(loop: asyncio.AbstractEventLoop) -> None:
    loop.set_exception_handler(lambda lp, ctx: None)
    loop.set_exception_handler(None)
    assert loop.get_exception_handler() is None


def test_exception_in_callback_routed_to_handler(loop: asyncio.AbstractEventLoop) -> None:
    seen: list[dict] = []

    async def main() -> None:
        loop.set_exception_handler(lambda lp, ctx: seen.append(ctx))

        def boom() -> None:
            raise RuntimeError("callback boom")

        loop.call_soon(boom)
        await asyncio.sleep(0.01)

    run(loop, main())
    assert any("callback boom" in str(ctx.get("exception")) for ctx in seen)


def test_exception_in_protocol_routed(loop: asyncio.AbstractEventLoop) -> None:
    seen: list[dict] = []

    async def main() -> None:
        loop.set_exception_handler(lambda lp, ctx: seen.append(ctx))

        class BadServer(asyncio.Protocol):
            def connection_made(self, transport: asyncio.BaseTransport) -> None:
                self.transport = transport

            def data_received(self, data: bytes) -> None:
                raise RuntimeError("protocol boom")

        server = await loop.create_server(BadServer, "127.0.0.1", 0)
        host, port = server.sockets[0].getsockname()
        lost: asyncio.Future[None] = loop.create_future()

        class Client(asyncio.Protocol):
            def connection_made(self, transport: asyncio.Transport) -> None:
                transport.write(b"trigger")
                self.transport = transport

            def connection_lost(self, exc: BaseException | None) -> None:
                if not lost.done():
                    lost.set_result(None)

        await loop.create_connection(Client, host, port)
        await asyncio.wait_for(lost, 2.0)
        server.close()
        await server.wait_closed()

    run(loop, main())
    assert any("protocol boom" in str(ctx.get("exception")) for ctx in seen)
