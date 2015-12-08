import contextlib
import cStringIO
import gzip


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
