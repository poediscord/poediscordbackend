import asyncio
import aioredis
import json
from dataclasses import dataclass, asdict
from poediscordcontroller.brokers import Broker, JobQueue
from poediscordcontroller.model import Job, JobId, JobResult, Initiator


@dataclass
class RedisJobQueue(JobQueue):
    job_chan: str
    result_chan: str
    jobs: str
    results: str


class Redis(Broker):

    @classmethod
    def job_queue_factory(cls, queue_name) -> RedisJobQueue:
        return RedisJobQueue(
            queue_name, 
            f"jq_{queue_name}_job_chan", 
            f"jq_{queue_name}_result_chan", 
            f"jq_{queue_name}_jobs", 
            f"jq_{queue_name}_results")
    

    async def connect(self, loop):
        self.conn = await aioredis.create_redis_pool(
            self.config['broker']["uri"],
            loop=loop,
            encoding='utf-8'
        )
        self.log.info(f"Connected to redis server: {self.config['broker']['uri']}")


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
        job_fqn = await self.conn.lpop(queue.job_chan)
        job_json = await self.conn.hget(queue.jobs, job_fqn)
        job = self.deserialize_job(job_json)
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

    def serialize_job(self, job:Job) -> str:
        root = {}
        root["job_id"] = {
            "queue": job.job_id.queue,
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
        root["task"] = job.task
        root["data"] = job.data
        return json.dumps(root)

    def deserialize_job(self, job_json:str) -> Job:
        root = json.loads(job_json)
        queue_name = root["job_id"]["queue"]
        if queue_name in self.job_queues:
            job_queue = self.job_queues[queue_name]
        else:
            raise Exception(f"Queue: {queue_name} not in:\n{self.job_queues}")
        job_id = JobId(
            job_queue, 
            root["job_id"]["job_id"])
        init = Initiator()
        task = root["task"]
        data = root["data"]
        stage_name = root["stage"]
        job = Job(job_id, init, task, data, stage_name)
        return job