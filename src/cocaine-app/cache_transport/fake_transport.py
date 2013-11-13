

class Transport(object):

    def __init__(self, *args, **kwargs):
        self.tasks = {}

    def put(self, task):
        self.tasks[task['key']] = task
