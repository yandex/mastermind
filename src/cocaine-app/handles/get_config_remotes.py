from handles import Handle
from handles.builder import FlatbuffersBuilder
import helpers as h
from mastermind_core.config import config
from mastermind_core.flatbuffers.mmc import InitialRemotesInfo


CONFIG_REMOTES = config.get('elliptics', {}).get('nodes', [])

RESPONSE_FORMAT_NATIVE = 'native'
RESPONSE_FORMAT_FLATBUFFERS = 'flatbuffers'


class GetConfigRemotesHandle(Handle):

    @h.handler_wne
    def __call__(self, request):
        request = request or {}
        response_format = request.get('format', RESPONSE_FORMAT_NATIVE)

        if response_format == RESPONSE_FORMAT_NATIVE:
            return self.get_config_remotes_native()
        elif response_format == RESPONSE_FORMAT_FLATBUFFERS:
            return self.get_config_remotes_flatbuffers()

        raise ValueError('Unsupported response format: "{}"'.format(response_format))

    def get_config_remotes_native(self):
        return CONFIG_REMOTES

    def get_config_remotes_flatbuffers(self):
        return ConfigRemotesFlatbuffersBuilder().build()


class ConfigRemotesFlatbuffersBuilder(FlatbuffersBuilder):

    def build(self):

        elliptics_remotes = [
            ':'.join((host, str(port), str(family)))
            for host, port, family in CONFIG_REMOTES
        ]

        fb_eliptics_remote_offsets = self._save_strings(elliptics_remotes)
        InitialRemotesInfo.InitialRemotesInfoStartEllipticsRemotesVector(
            self.builder,
            len(fb_eliptics_remote_offsets)
        )
        for i in reversed(fb_eliptics_remote_offsets):
            self.builder.PrependUOffsetTRelative(i)
        fb_elliptics_remotes_offset = self.builder.EndVector(len(fb_eliptics_remote_offsets))

        InitialRemotesInfo.InitialRemotesInfoStart(self.builder)
        # TODO: add timestamp
        InitialRemotesInfo.InitialRemotesInfoAddEllipticsRemotes(self.builder, fb_elliptics_remotes_offset)
        root_offset = InitialRemotesInfo.InitialRemotesInfoEnd(self.builder)

        self.builder.Finish(root_offset)

        return str(self.builder.Output())


handle = GetConfigRemotesHandle('get_config_remotes')


def get_config_remotes(request):
    return handle(request)
