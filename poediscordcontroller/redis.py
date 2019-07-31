import asyncio
import aioredis
from poediscordcontroller.broker import Broker, JobQueue
from poediscordcontroller.model import Job


@dataclass
class RedisJobQueue(JobQueue):
    job_chan: str
    result_chan: str
    jobs: str
    results: str


class Redis(Broker):

    def __init__(self, config):
        super().__init__config()
        self.job_queues = {}

    async def connect(self):
        self.conn = await aioredis.create_connection(
            self.config["uri"],
            encoding='utf-8'
        )
        log.info(f"Connected to redis server: {self.config["uri"]}")

    def register_job_queue(self, name) -> JobQueue:
        """Job queues should be two part. a Fifo queue and a table of active ones."""
        queue = JobQueue(name, f"jq_{name}_job_chan", f"jq_{name}_result_chan", f"jq_{name}_jobs", f"jq_{name}_results")
        self.job_queues[name] = queue
        return queue

    async def add_job(self, queue:JobQueue, job:Job):
        tr = self.conn.multi_exec()
        fut_job = tr.hset(queue.jobs, job.fqn, job)
        fut_pub = tr.push(queue.job_chan, job.fqn)
        result = await tr.execute()

    async def aquire_job(self, queue:JobQueue) -> Job:
        """
        pull a job out of the fifo, grab the data.
        """
        tr = self.conn.multi_exec()
        fut_pub = tr.pop(queue.job_chan, job.fqn)
        fut_job = tr.hget(queue.jobs, job.fqn)
        jid, job = await tr.execute()
        return job

    async def update_job(self, job):
        """Update the data in an active job"""
        return await self.conn.hset(self.job_queues[job.job_id.queue].jobs, job.job_id.fqn, job)

    async def complete_job(self, job, result):
        """Publish a jobs result and remove it from active"""
        queue = self.job_queues[job.job_id.queue]
        tr = self.conn.multi_exec()
        fut_rem = tr.hdel(queue.jobs, job.job_id.fqn)
        fur_res = tr.hset(queue.results, job.job_id.fqn, result)
        _, res = await tr.execute()
        return res
