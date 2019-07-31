import abc
import logging
from dataclasses import dataclass
from poediscordcontroller.model import Job


@dataclass
class JobQueue:
    name: str
    pass

class Broker(abc.ABC):
    def __init__(self, config):
        self.log = logging.getLogger(type(self).__qualname__)
        self.config = config

    @abc.abstractmethod
    async def connect(self, config):
        pass

    @abc.abstractmethod
    def register_job_queue(self, name) -> JobQueue:
        """Job queues should be two part. a Fifo queue and a table of active ones."""
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
    async def complete_job(self, queue, job, result):
        """Publish a jobs result and remove it from active"""
        pass