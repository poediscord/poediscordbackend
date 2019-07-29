import asyncio
from dataclasses import dataclass

from poediscordcontroller import dispatch
from poediscordcontroller.model import Job, Initiator, JobResult

def test_dispatch():
    dispatcher = dispatch.CommandDispatcher("db", "broker")

    output = []

    class SayCommandHandler(dispatch.CommandHandler):
        async def begin(self):
            output.append(self.data.message)
            return JobResult.Complete

    @SayCommandHandler.handles
    @dataclass
    class SayCommand(dispatch.Command):
        message: str

    init = Initiator()
    cmd = SayCommand("Hi")
    job = Job(job_id=1, initiator=init, data=cmd)
    result = asyncio.run(dispatcher.dispatch(job))

    assert output == ["Hi"]
