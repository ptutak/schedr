"""Output types and values: annotations to type descriptors, and values to stored JSON.

Three checks make sure an output can be serialized (see PROJECT.md, Output serialization):
the type checker (``Json`` bound on ``@task``), the parse-time check (:func:`describe`), and
the run-time check (:func:`encode`).
"""

from schedr._serialization.annotations import TypeModelError
from schedr._serialization.codec import (
    CodecError,
    check_size,
    decode,
    decode_any,
    encode,
    encode_any,
    encoded_size,
)
from schedr._serialization.compatibility import follow_path, is_assignable, type_name
from schedr._serialization.describe import describe

__all__ = [
    "CodecError",
    "TypeModelError",
    "check_size",
    "decode",
    "decode_any",
    "describe",
    "encode",
    "encode_any",
    "encoded_size",
    "follow_path",
    "is_assignable",
    "type_name",
]
