import pytest
import json
from poediscordcontroller.model import Data, Initiator, Job, JobId
from poediscordcontroller.redis import Redis, RedisJobQueue
from uuid import uuid4

@pytest.fixture
def job_queue(broker):
    randoname = uuid4()
    return broker.register_job_queue(randoname)

def test_connection(broker, loop):
    async def ping_server():
        return await broker.conn.execute("PING", "pung"), 
    assert ('pung',) == loop.run_until_complete(ping_server())

def test_add_queue(broker, loop):
    randoname = uuid4()
    queue = broker.register_job_queue(randoname)
    assert queue.name == randoname
    assert queue.job_chan == f"jq_{randoname}_job_chan"
    assert queue.result_chan == f"jq_{randoname}_result_chan"
    assert queue.jobs == f"jq_{randoname}_jobs"
    assert queue.results == f"jq_{randoname}_results"

def test_add_job(broker, job_queue, loop):
    job_id = JobId(job_queue.name, uuid4().int)
    init = Initiator()
    job = Job(job_id, init, Data(), None)

    res = loop.run_until_complete(broker.add_job(job))

    job_stored = loop.run_until_complete(broker.conn.execute("hget", job_queue.jobs, job_id.fqn))

    assert json.loads(job_stored) == {
        "job_id": {
            "queue": job_id.queue.hex,
            "job_id": job_id.job_id
        }, 
        "initiator": {},
        "stage": None,
        "data": {},
        "data_type": "Data"
    }