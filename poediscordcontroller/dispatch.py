import abc
import logging
from dataclasses import dataclass
from datetime import datetime
from poediscordcontroller.model import Job, Data, JobResult


class Dispatcher(abc.ABC):
    def __init__(self, db, broker):
        self.db = db
        self.broker = broker
        self.log = logging.getLogger(type(self).__qualname__)

    async def monitor_queue(self, queue_name):
        if queue_name in self.broker.job_queues:
            queue = self.broker.job_queues[queue_name]
        else:
            queue = self.broker.register_job_queue(queue_name)

        while True:
            job = await self.broker.aquire_job(queue)
            result = await self.dispatch(job)
            await self.broker.complete_job(queue, job, result)

    @abc.abstractmethod
    async def dispatch(self, job: Job):
        pass

    async def execute(self, handler):
        async for result in handler.execute():
            await self.save_state(handler.job)
        return result

    @abc.abstractmethod
    async def save_state(self, job: Job):          
        pass

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
            self.job.stage = self.begin
        try:
            while hasattr(self.job.stage, "__call__"): #TODO: make cancelable
                self.job.stage = await self.job.stage()
                yield self.job.stage
        except Exception as e:
            self.log.error(f"Error handling {type(self.data).__qualname__}: {e}")
            raise e
        finally:
            end = datetime.utcnow()
            self.log.debug(f"Response for {type(self.data).__qualname__} served in {end-start} seconds")

    @abc.abstractmethod
    async def begin(self, params):
        pass
        
    @abc.abstractclassmethod
    def handles(handler_cls, query_cls):
        """Decorator to associate a handler with a query"""
        pass


"""Queries"""

_queries = {}

class QueryDispatcher(Dispatcher):
    """System that runs queries"""

    async def dispatch(self, job: Job):
        query = job.data
        handler = _queries[type(query).__qualname__]
        # pass our db and broker to the handler and then execute the query
        return await self.execute(handler(self.db, self.broker, job))

    async def save_state(self, handler):
        #await self.broker.queries.active.update(self.id, stage=self.stage, data=data)
        pass

@dataclass
class Query(Data):
    """Query base class. Subclasses will have the parameters for the query"""
    pass

class QueryHandler(Handler):

    @classmethod
    def handles(handler_cls, query_cls):
        """Decorator to associate a handler with a query"""
        query_cls.handles = query_cls
        _queries[query_cls.__qualname__] = handler_cls
        return query_cls

"""Commands"""

_commands = {}

class CommandDispatcher(Dispatcher):
    """System that runs queries"""

    async def dispatch(self, job):
        command = job.data
        handler = _commands[type(command).__qualname__]
        # pass our db and broker to the handler and then execute the command
        return await self.execute(handler(self.db, self.broker, job))

    async def save_state(self, handler):
        """await self.broker.commands.active.update(
            handler.id, 
            handler.stage, 
            handler.data)"""
        pass

@dataclass
class Command(Data):
    """Command base class. Subclasses will have the parameters for the command"""
    pass

class CommandHandler(Handler):

    @classmethod
    def handles(handler_cls, command_cls):
        """Decorator to associate a handler with a command"""
        command_cls.handles = command_cls
        _commands[command_cls.__qualname__] = handler_cls
        return command_cls
