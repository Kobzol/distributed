import logging
import operator

import msgpack

try:
    from cytoolz import reduce
except ImportError:
    from toolz import reduce

from .compression import compressions, maybe_compress, decompress
from .serialize import serialize, deserialize, Serialize, Serialized, extract_serialize
from .utils import frame_split_size, merge_frames, msgpack_opts
from ..utils import nbytes

_deserialize = deserialize


logger = logging.getLogger(__name__)


OFFLOAD_HEADER_KEY = "_$header"
OFFLOAD_FINDEX_KEY = "_$findex"
OFFLOAD_FCOUNT_KEY = "_$fcount"


def _make_offload_value(header, frame_index, frame_count):
    return {OFFLOAD_HEADER_KEY: header, OFFLOAD_FINDEX_KEY: frame_index, OFFLOAD_FCOUNT_KEY: frame_count}


def _extract_offload_value(value):
    if not isinstance(value, dict) or len(value) != 3:
        return None
    frame_index = value.get(OFFLOAD_FINDEX_KEY)
    if frame_index is None:
        return None
    header = value.get(OFFLOAD_HEADER_KEY)
    if header is None:
        return None
    assert header["count"] == value.get(OFFLOAD_FCOUNT_KEY)  # TODO
    return (header, frame_index)


def dumps(msg, serializers=None, on_error="message", context=None):
    """ Transform Python message to bytestream suitable for communication """
    try:
        data = {}
        # Only lists and dicts can contain serialized values
        if isinstance(msg, (list, dict)):
            msg, data, bytestrings = extract_serialize(msg)

        if not data:  # fast path without serialized data
            return dumps_msgpack(msg)

        pre = {
            key: (value.header, value.frames)
            for key, value in data.items()
            if type(value) is Serialized
        }

        data = {
            key: serialize(
                value.data, serializers=serializers, on_error=on_error, context=context
            )
            for key, value in data.items()
            if type(value) is Serialize
        }

        out_frames = []

        def patch_offload_header(path, header, frame_index, frame_count, context):
            accessor, key = path[:-1], path[-1]
            holder = reduce(operator.getitem, accessor, context)
            header["deserialize"] = path in bytestrings
            holder[key] = _make_offload_value(header, frame_index, frame_count)

        for key, (head, frames) in data.items():
            head = dict(head)
            if "lengths" not in head:
                head["lengths"] = tuple(map(nbytes, frames))
            if "compression" not in head:
                frames = frame_split_size(frames)
                if frames:
                    compression, frames = zip(*map(maybe_compress, frames))
                else:
                    compression = []
                head["compression"] = compression
            head["count"] = len(frames)
            patch_offload_header(key, head, len(out_frames), len(frames), msg)
            out_frames.extend(frames)

        for key, (head, frames) in pre.items():
            head = dict(head)
            if "lengths" not in head:
                head["lengths"] = tuple(map(nbytes, frames))
            head["count"] = len(frames)
            patch_offload_header(key, head, len(out_frames), len(frames), msg)
            out_frames.extend(frames)

        for i, frame in enumerate(out_frames):
            if type(frame) is memoryview and frame.strides != (1,):
                try:
                    frame = frame.cast("b")
                except TypeError:
                    frame = frame.tobytes()
                out_frames[i] = frame

        return dumps_msgpack(msg) + out_frames
    except Exception:
        logger.critical("Failed to Serialize", exc_info=True)
        raise


def loads(frames, deserialize=True, deserializers=None):
    """ Transform bytestream back into Python value """
    if not isinstance(frames, list):
        frames = list(frames)
    try:
        small_header = frames[0]
        small_payload = frames[1]
        msg = loads_msgpack(small_header, small_payload)
        if len(frames) < 3:
            return msg

        out_frames_start = 2

        def _traverse(item):
            placeholder = _extract_offload_value(item)
            if placeholder is not None:
                header, frame_index = placeholder
                deserialize_key = header["deserialize"]
                count = header["count"]
                if count:
                    start_index = out_frames_start + frame_index
                    end_index = start_index + count
                    fs = frames[start_index:end_index]
                    frames[start_index:end_index] = [None] * count  # free memory
                else:
                    fs = []

                if deserialize or deserialize_key:
                    if "compression" in header:
                        fs = decompress(header, fs)
                    fs = merge_frames(header, fs)
                    value = _deserialize(header, fs, deserializers=deserializers)
                else:
                    value = Serialized(header, fs)
                return value

            if isinstance(item, (list, tuple)):
                return list(_traverse(i) for i in item)
            elif isinstance(item, dict):
                return {
                    key: _traverse(val)
                    for (key, val) in item.items()
                }
            else:
                return item

        return _traverse(msg)
    except Exception:
        logger.critical("Failed to deserialize", exc_info=True)
        raise


def dumps_msgpack(msg):
    """ Dump msg into header and payload, both bytestrings

    All of the message must be msgpack encodable

    See Also:
        loads_msgpack
    """
    header = {}
    payload = msgpack.dumps(msg, use_bin_type=True)

    fmt, payload = maybe_compress(payload)
    if fmt:
        header["compression"] = fmt

    if header:
        header_bytes = msgpack.dumps(header, use_bin_type=True)
    else:
        header_bytes = b""

    return [header_bytes, payload]


def loads_msgpack(header, payload):
    """ Read msgpack header and payload back to Python object

    See Also:
        dumps_msgpack
    """
    header = bytes(header)
    if header:
        header = msgpack.loads(header, use_list=False, **msgpack_opts)
    else:
        header = {}

    if header.get("compression"):
        try:
            decompress = compressions[header["compression"]]["decompress"]
            payload = decompress(payload)
        except KeyError:
            raise ValueError(
                "Data is compressed as %s but we don't have this"
                " installed" % str(header["compression"])
            )

    return msgpack.loads(payload, use_list=False, **msgpack_opts)
