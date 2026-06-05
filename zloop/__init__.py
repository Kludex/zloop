from __future__ import annotations

import asyncio

from zloop._zloop import Handle, Loop, TimerHandle

__all__ = ["Handle", "Loop", "TimerHandle", "new_event_loop"]


def new_event_loop() -> asyncio.AbstractEventLoop:
    """Create a new zloop event loop, suitable as an asyncio loop factory."""
    return Loop()
