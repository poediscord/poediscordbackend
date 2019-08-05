import asyncio
from dataclasses import dataclass

from poediscordcontroller import dispatch
from poediscordcontroller import brokers
from poediscordcontroller.model import Job, Initiator, JobResult

def test_register_dispatcher(broker, dispatcher_cls, dispatcher, queue_name):
    assert queue_name in broker.dispatchers
    assert broker.dispatchers[queue_name] is dispatcher_cls
    assert hasattr(dispatcher_cls, "handlers")
    assert hasattr(dispatcher, "handlers")
    assert dispatcher_cls.handlers is dispatcher.handlers

def test_register_handler(broker, dispatcher_cls, dispatcher, loop):

    output = []

    @dispatch.register_handler(dispatcher_cls, "say")
    class SayCommandHandler(dispatch.Handler):
        async def stage_begin(self):
            output.append(self.data["message"])
            return JobResult.Complete

    init = Initiator()
    task = "say"
    command = {"message":"Hi"}
    job = Job(job_id=1, initiator=init, task=task, data=command)

    async def dispatch_loop():
        async for result in dispatcher.dispatch(None, broker, job):
            pass

    result = loop.run_until_complete(dispatch_loop())

    assert output == ["Hi"]
