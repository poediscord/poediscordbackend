import asyncio
import aioredis
import json
from dataclasses import dataclass, asdict
from poediscordcontroller.broker import Broker, JobQueue
from poediscordcontroller.model import Job, JobResult


@dataclass
class RedisJobQueue(JobQueue):
    job_chan: str
    result_chan: str
    jobs: str
    results: str


class Redis(Broker):

    def __init__(self, config):
        super().__init__(config)
        self.job_queues = {}

    async def connect(self, loop):
        self.conn = await aioredis.create_redis_pool(
            self.config['broker']["uri"],
            loop=loop,
            encoding='utf-8'
        )
        self.log.info(f"Connected to redis server: {self.config['broker']['uri']}")

    def register_job_queue(self, name) -> RedisJobQueue:
        """Job queues should be two part. a Fifo queue and a table of active ones."""
        queue = RedisJobQueue(name, f"jq_{name}_job_chan", f"jq_{name}_result_chan", f"jq_{name}_jobs", f"jq_{name}_results")
        self.job_queues[name] = queue
        return queue

    async def add_job(self, job:Job):
        tr = self.conn.multi_exec()
        queue = self.job_queues[job.job_id.queue]
        job_serialized = self.serialize_job(job)
        fut_job = tr.hset(queue.jobs, job.job_id.fqn, job_serialized)
        fut_pub = tr.lpush(queue.job_chan, job.job_id.fqn)
        result = await tr.execute()

    async def aquire_job(self, queue:RedisJobQueue) -> Job:
        """
        pull a job out of the fifo, grab the data.
        """
        tr = self.conn.multi_exec()
        fut_pub = tr.lpop(queue.job_chan, job.job_id.fqn)
        fut_job = tr.hget(queue.jobs, job.job_id.fqn)
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

    async def list_jobs(self, queue:RedisJobQueue):
        return await self.conn.execute("hgetall", queue.jobs)

    async def list_results(self, queue:RedisJobQueue):
        return await self.conn.execute("hgetall", queue.results)

    def serialize_job(self, job:Job) -> bytes:
        root = {}
        root["job_id"] = {
            "queue": job.job_id.queue.hex,
            "job_id": job.job_id.job_id
        }
        root["initiator"] = asdict(job.initiator)
        if hasattr(job.stage, "__call__"):
            stage = job.stage.__name__
        elif isinstance(job.stage, JobResult):
            stage = job.stage.value
        else:
            stage = None
        root["stage"] = stage
        root["data"] = asdict(job.data)
        root["data_type"] = job.data.__class__.__name__
        return json.dumps(root)