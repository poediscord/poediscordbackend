import abc
import logging
import inspect
from dataclasses import dataclass
from poediscordcontroller import model
from poediscordcontroller.model import Job


@dataclass
class JobQueue:
    name: str

class BrokerMeta(abc.ABCMeta):
    def __init__(cls, name, bases, dct):
        cls.dispatchers = {}
        cls.job_queues = {}

class Broker(metaclass=BrokerMeta):
    def __init__(self, config):
        self.log = logging.getLogger(type(self).__qualname__)
        self.config = config

    @abc.abstractclassmethod
    def job_queue_factory(cls, queue_name) -> JobQueue:
        """Simply build a JobQueue instance appropreate for your broker subclass."""
        pass

    @abc.abstractmethod
    async def connect(self, config):
        """Create the connection to the broker"""
        pass
    
    @abc.abstractmethod
    async def aquire_job(self, queue: JobQueue):
        """
        pull a job out of the fifo, move it in to the active and return it.
        """
        pass

    @abc.abstractmethod
    async def update_job(self, queue:JobQueue, job:Job):
        """Update the data in an active job"""
        pass
    
    @abc.abstractmethod
    async def complete_job(self, queue:JobQueue, job:Job, result):
        """Publish a jobs result and remove it from active"""
        pass

    @abc.abstractmethod
    def serialize_job(self, job:Job) -> str:
        """Convert a job to a json string"""
        pass

    @abc.abstractmethod
    def deserialize_job(self, job_json:str) -> Job:
        """Convert a json string to a Job object"""
        pass

class register_dispatcher:
    """Register this queue and associate a dispatcher with it"""

    def __init__(self, broker_class, queue_name):
        self.broker_class = broker_class
        self.queue_name = queue_name

    def __call__(self, dispatcher_class):
        job_queue = self.broker_class.job_queue_factory(self.queue_name)
        self.broker_class.job_queues[self.queue_name] = job_queue
        self.broker_class.dispatchers[self.queue_name] = dispatcher_class
        return dispatcher_class