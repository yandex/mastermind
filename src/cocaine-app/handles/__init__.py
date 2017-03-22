class Handle(object):

    def __init__(self, handle_name):
        self.handle_name = handle_name

    def __call__(self, request):
        raise NotImplementedError(
            'Handle "{}" is not implemented'.format(self.handle_name)
        )

