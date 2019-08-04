import pytest
import asyncio
import aioredis

from poediscordcontroller import main
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
        conn = Redis(config)
        loop.run_until_complete(conn.connect(loop))
#        loop.run_until_complete(conn.flushdb())
        return conn
    else:
        raise Exception("Unsupported broker type in config")