from dataclasses import dataclass, field
from typing import List, Dict, Optional
import uuid

# job class
@dataclass
class Job:
    # required first
    name: str
    minutes: int
    cpus: int

    # optional with defaults
    gpus: int = 0
    priority: int = 0
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    state: str = "PENDING"                     # PENDING, RUNNING, DONE, CANCELED
    remaining: int = 0                         # initialized from minutes in __post_init__
    assigned_node: Optional[str] = None
    submit_time: int = 0
    start_time: Optional[int] = None
    end_time: Optional[int] = None

    def __post_init__(self):
        if self.remaining == 0:
            self.remaining = self.minutes


# node class
@dataclass
class Node:
    id: str
    total_cpus: int
    total_gpus: int
    used_cpus: int = 0
    used_gpus: int = 0
    running_jobs: List[str] = field(default_factory=list)

    def can_fit(self, job: Job) -> bool:
        return ((self.total_cpus - self.used_cpus) >= job.cpus and
                (self.total_gpus - self.used_gpus) >= job.gpus)

    def assign(self, job: Job):
        self.used_cpus += job.cpus
        self.used_gpus += job.gpus
        self.running_jobs.append(job.id)

    def release(self, job: Job):
        if job.id in self.running_jobs:
            self.running_jobs.remove(job.id)
            self.used_cpus -= job.cpus
            self.used_gpus -= job.gpus


# cluster class
@dataclass
class Cluster:
    nodes: Dict[str, Node] = field(default_factory=dict)
    now: int = 0  # minutes since simulation start

    def add_nodes(self, n: int, cpus_per_node: int, gpus_per_node: int):
        for i in range(n):
            nid = f"N{i+1}"
            self.nodes[nid] = Node(
                id=nid,
                total_cpus=cpus_per_node,
                total_gpus=gpus_per_node,
            )

    def total_utilization(self):
        cpu_used = sum(n.used_cpus for n in self.nodes.values())
        cpu_total = sum(n.total_cpus for n in self.nodes.values())
        gpu_used = sum(n.used_gpus for n in self.nodes.values())
        gpu_total = sum(n.total_gpus for n in self.nodes.values())
        cpu_util = (cpu_used / cpu_total * 100) if cpu_total else 0.0
        gpu_util = (gpu_used / gpu_total * 100) if gpu_total else 0.0
        return cpu_util, gpu_util
