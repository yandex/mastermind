

class Transport(object):

    def __init__(self, *args, **kwargs):
        self.tasks = []

    def put_task(self, task):
        self.tasks.append(task)

    def put_all(self, tasks):
        for task in tasks:
            self.put_task(task)

    def list(self):
        return self.tasks
