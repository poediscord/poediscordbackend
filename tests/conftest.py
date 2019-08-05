import pytest
import asyncio
import aioredis
from uuid import uuid4

from poediscordcontroller import main
from poediscordcontroller import dispatch
from poediscordcontroller import brokers
from poediscordcontroller import model
from poediscordcontroller.redis import Redis

@pytest.fixture
def config():
    main.load_config(testing=True)
    return main.config

@pytest.fixture
def loop():
    return asyncio.get_event_loop()

@pytest.fixture
def broker(config, loop):
    if config["broker"]["type"] == "redis":
        redis = Redis(config)
        loop.run_until_complete(redis.connect(loop))
        loop.run_until_complete(redis.conn.flushdb())
        return redis
    else:
        raise Exception("Unsupported broker type in config")

@pytest.fixture
def queue_name():
    return uuid4().hex

@pytest.fixture
def dispatcher_cls(broker, queue_name):
    @brokers.register_dispatcher(broker, queue_name)
    class TestDispatcher(dispatch.Dispatcher):
        pass

    return TestDispatcher

@pytest.fixture
def dispatcher(broker, dispatcher_cls, queue_name):
    return dispatcher_cls()

@pytest.fixture
def job_queue(broker, dispatcher, queue_name):
    return broker.job_queues[queue_name]

@pytest.fixture
def job(job_queue):
    job_id = model.JobId(job_queue.name, uuid4().int)
    init = model.Initiator()
    job = model.Job(job_id, init, "test", {}, None)
    return job