"""The runner protocol's framing.

Shared with extensions' plugins: it lives in :mod:`schedr_extension._protocol`, which imports
nothing of ``schedr`` (B283).
"""

from schedr_extension._protocol import (
    FD_ENV,
    MAX_MESSAGE_SIZE,
    Asking,
    Channel,
    ChannelClosedError,
    Message,
    ProtocolError,
    Telling,
    Wire,
    as_wire,
    flag,
    from_fd,
    listed,
    obj,
    optional_whole,
    text,
    texts,
    whole,
)

__all__ = [
    "FD_ENV",
    "MAX_MESSAGE_SIZE",
    "Asking",
    "Channel",
    "ChannelClosedError",
    "Message",
    "ProtocolError",
    "Telling",
    "Wire",
    "as_wire",
    "flag",
    "from_fd",
    "listed",
    "obj",
    "optional_whole",
    "text",
    "texts",
    "whole",
]
