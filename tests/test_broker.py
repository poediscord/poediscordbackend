import pytest
import json
from poediscordcontroller.model import Initiator, Job, JobId
from poediscordcontroller.redis import Redis, RedisJobQueue

def test_connection(broker, loop):
    async def ping_server():
        return await broker.conn.execute("PING", "pung"), 
    assert ('pung',) == loop.run_until_complete(ping_server())

def test_add_queue(broker, job_queue, loop):
    assert job_queue.job_chan == f"jq_{job_queue.name}_job_chan"
    assert job_queue.result_chan == f"jq_{job_queue.name}_result_chan"
    assert job_queue.jobs == f"jq_{job_queue.name}_jobs"
    assert job_queue.results == f"jq_{job_queue.name}_results"

    assert broker.job_queues[job_queue.name] is job_queue

def test_add_job(broker, job_queue, job, loop):
    job_id = job.job_id

    async def add_and_grab():
        res = await broker.add_job(job)
        job_stored = await broker.conn.execute(
            "hget", job_queue.jobs, job_id.fqn
        )
        job_chan_len = await broker.conn.llen(
            job_queue.job_chan
        )
        job_chan_stored = await broker.conn.lrange(
            job_queue.job_chan, 0, -1
        )
        return res, job_stored, job_chan_len, job_chan_stored

    res, job_stored, job_chan_len, job_chan_stored = \
        loop.run_until_complete(add_and_grab())

    assert json.loads(job_stored) == {
        "job_id": {
            "queue": job_id.queue,
            "job_id": job_id.job_id
        }, 
        "initiator": {},
        "stage": None,
        "task": "test",
        "data": {},
    }

    assert job_chan_len == 1

    assert job_chan_stored == [job_id.fqn]

def test_aquire_job(broker, job_queue, job, loop):
    job_id = job.job_id

    async def add_job():
        res = await broker.add_job(job)
        job_res = await broker.aquire_job(job_queue)

        job_stored = await broker.conn.execute(
            "hget", job_queue.jobs, job_id.fqn
        )
        job_chan_len = await broker.conn.llen(
            job_queue.job_chan
        )
        job_chan_stored = await broker.conn.lrange(
            job_queue.job_chan, 0, -1
        )
        return res, job_stored, job_chan_len, job_chan_stored, job_res

    res, job_stored, job_chan_len, job_chan_stored, job_res = \
        loop.run_until_complete(add_job())

    assert json.loads(job_stored) == {
        "job_id": {
            "queue": job_id.queue,
            "job_id": job_id.job_id
        }, 
        "initiator": {},
        "stage": None,
        "task": "test",
        "data": {},
    }

    assert job_chan_len == 0

    assert job_chan_stored == []