import collections

import flatbuffers


DEFAULT_INITIAL_FB_SIZE = 10 * (1024 ** 2)  # 10 MBs


class FlatbuffersBuilder(object):
    def __init__(self, initial_fb_size=None):
        self.builder = flatbuffers.Builder(initial_fb_size or DEFAULT_INITIAL_FB_SIZE)

        self._string_offsets = {}  # maps string to it's offset inside flatbuffers builder

    def _save_string(self, s, shared=False):
        assert isinstance(s, basestring)

        if not shared:
            return self.builder.CreateString(s)

        offset = self._string_offsets.get(s)
        if offset is None:
            offset = self.builder.CreateString(s)
            self._string_offsets[s] = offset
        return offset

    def _save_strings(self, strs, shared=False):
        assert isinstance(strs, collections.Iterable)
        return [self._save_string(s, shared=shared) for s in strs]
