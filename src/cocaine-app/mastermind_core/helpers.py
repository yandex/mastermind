import contextlib
import copy
import cStringIO
import gzip
from json import JSONEncoder


def gzip_compress(data, compression_level=1):
    with contextlib.closing(cStringIO.StringIO()) as buf:
        gz = gzip.GzipFile(
            fileobj=buf,
            mode='wb',
            compresslevel=compression_level
        )
        with gz:
            gz.write(data)
        return buf.getvalue()


def encode(s, encoding='utf-8', errors='strict'):
    """Returns an encoded version of string

    NB: s is encoded only if it was a unicode string, otherwise
    it is returned as is.
    """
    if isinstance(s, unicode):
        return s.encode(encoding, errors=errors)
    return s


def merge_dict(dst, src):
    """ Merges two dicts updating 'dst' keys with those from 'src'
    """
    res = copy.deepcopy(dst)
    for k, val in src.iteritems():
        if k not in dst:
            res[k] = val
        else:
            if not isinstance(val, dict):
                res[k] = val
            else:
                res[k] = merge_dict(res[k], src[k])
    return res


def percent(val):
    return '{:.2f}%'.format(val * 100.0)


BYTES_UNITS = ('B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB')


def convert_bytes(b):
    res = float(b)
    for unit in BYTES_UNITS[:-1]:
        if abs(res) < 1024:
            return '{:.2f} {}'.format(res, unit)
        res = res / 1024
    return '{:.2f} {}'.format(res, BYTES_UNITS[-1])


CONFIG_BYTES_UNITS = ('T', 'G', 'M', 'K')


def convert_config_bytes_value(value):

    format_error_msg = (
        'Unexpected bytes values: {}, expected an integer value optionally followed by one of '
        '("T", "G", "M", "K")'
    )

    if isinstance(value, (int, long)):
        return value

    if not value:
        raise ValueError(format_error_msg)

    if value[-1].upper() in CONFIG_BYTES_UNITS:
        bytes_part, suffix = value[:-1], value[-1].upper()
    else:
        bytes_part, suffix = value, None

    try:
        bytes_value = int(bytes_part)
    except ValueError:
        raise ValueError(format_error_msg)

    if suffix == 'K':
        return bytes_value * (1024 ** 1)
    elif suffix == 'M':
        return bytes_value * (1024 ** 2)
    elif suffix == 'G':
        return bytes_value * (1024 ** 3)
    elif suffix == 'T':
        return bytes_value * (1024 ** 4)
    return bytes_value


def json_dumps(obj):
    """ Dumps obj to json representation.

    This implementation is based on standard library's json.JSONEncoder 'encode' method
    with an intent to force python implementation of encoding.
    This is done deliberately to assure that GIL is released and context is being switched
    during large objects' encoding.
    """
    encoder = JSONEncoder(check_circular=False)

    # This is for extremely simple cases and benchmarks.
    if isinstance(obj, basestring):
        return encoder.encode(obj)

    # This doesn't pass the iterator directly to ''.join() because the
    # exceptions aren't as detailed.  The list call should be roughly
    # equivalent to the PySequence_Fast that ''.join() would do.
    chunks = encoder.iterencode(obj, _one_shot=False)
    if not isinstance(chunks, (list, tuple)):
        chunks = list(chunks)
    return ''.join(chunks)
