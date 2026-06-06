"""Type stubs for the Zig-backed `_zloop` extension module."""

from __future__ import annotations

import asyncio
from typing import Any

class Handle:
    def cancel(self) -> None: ...
    def cancelled(self) -> bool: ...

class TimerHandle(Handle):
    def when(self) -> float: ...

# Presented as a concrete BaseEventLoop: at runtime the C extension implements
# the full AbstractEventLoop surface, so subclassing it is not abstract.
class Loop(asyncio.BaseEventLoop):
    def _make_transport(self, fd: int, protocol: Any, extra: dict[str, Any]) -> Any: ...

def new_event_loop() -> asyncio.AbstractEventLoop: ...
