from dataclasses import dataclass
from typing import Optional, Callable, Union
from enum import Enum

@dataclass
class Initiator:
    pass

@dataclass
class Data:
    pass

class JobResult(Enum):
    Complete = 0
    Delayed = 1
    Failed = 2

@dataclass
class Job:
    job_id: int
    initiator: Initiator
    data: Data
    stage: Union[Callable, JobResult, None] = None

