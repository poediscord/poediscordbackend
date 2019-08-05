import abc
import logging
from dataclasses import dataclass
from datetime import datetime
from poediscordcontroller.model import Job, JobResult

class DispatchException(Exception):
    pass

class DispatcherMeta(type):
    def __init__(cls, name, bases, dct):
        super().__init__(name, bases, dct)
        cls.handlers = {}

class Dispatcher(metaclass=DispatcherMeta):
    def __init__(self):
        self.log = logging.getLogger(f"{type(self).__qualname__}")

    async def dispatch(self, db, broker, job: Job):
        task = job.task

        if task in self.handlers:
            handler_class = self.handlers[task]
        else:
            raise DispatchException(
                f"Dispatcher for queue {self.__qualname__} received unmapped task {task}!")
        handler = handler_class(db, broker, job)
        async for result in handler.execute():
            yield result

class register_handler:
    """Associate this handler with the task on that dispatcher"""

    def __init__(self, dispatcher_class, task):
        self.dispatcher_class = dispatcher_class
        self.task = task

    def __call__(self, handler_class):
        self.dispatcher_class.handlers[self.task] = handler_class
        return handler_class

class Handler(abc.ABC):
    def __init__(self, db, broker, job):
        super().__init__()
        self.log = logging.getLogger(type(self).__qualname__)

        self.db = db
        self.broker = broker
        self.job = job
        self.data = job.data

    async def execute(self):
        start = datetime.utcnow()
        if not self.job.stage:
            self.job.stage = "begin"
        try:
            while isinstance(self.job.stage, str): #TODO: make cancelable
                self.job.stage = await getattr(self, f"stage_{self.job.stage}")()
                yield self.job.stage
        except Exception as e:
            self.log.error(f"Error handling {type(self.data).__qualname__}: {e}")
            raise e
        finally:
            end = datetime.utcnow()
            self.log.debug(f"Response for {type(self.data).__qualname__} served in {end-start} seconds")

    @abc.abstractmethod
    async def stage_begin(self, params):
        pass