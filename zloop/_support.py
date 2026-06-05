from __future__ import annotations

import sys
import traceback
from typing import Any


def default_exception_handler(context: dict[str, Any]) -> None:
    """Format and print an unhandled-callback context like asyncio does.

    Mirrors `asyncio.BaseEventLoop.default_exception_handler`: build a message
    from the context and write it (with any traceback) to stderr.
    """
    message = context.get("message")
    if not message:
        message = "Unhandled exception in event loop"

    exception = context.get("exception")
    exc_info: Any
    if exception is not None:
        exc_info = (type(exception), exception, exception.__traceback__)
    else:
        exc_info = False

    log_lines = [message]
    for key in sorted(context):
        if key in {"message", "exception"}:
            continue
        value = context[key]
        log_lines.append(f"{key}: {value!r}")

    print("\n".join(log_lines), file=sys.stderr)
    if exc_info:
        traceback.print_exception(*exc_info, file=sys.stderr)
