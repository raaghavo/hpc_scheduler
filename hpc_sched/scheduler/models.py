from dataclasses import dataclass, field
import uuid
from typing import List, Dict

# defines the job class
@dataclass
class Job:
    name: str
    minutes: int
    cpus: int
    gpus: int
    state: str
    start_time: str
    end_time: str
    id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    priority: int

# defines the node class
@dataclass
class Node:
    hostname: str
    gpus: int
    cpus: int
    used_cpus: int
    used_gpus: int
    current_jobs: List[str] = field(default_factory=list)

    # function to check if we can fit a job
    def can_fit(self, job: Job) -> bool:
        return (self.cpus - self.used_cpus) >= job.cpus and (self.gpus - self.used_gpus) >= job.gpus
    
    # function to assign a job to a node
    def assign_job(self, job: Job):
        self.used_cpus+=job.cpus
        self.used_gpus+=job.gpus
        self.current_jobs.append(job)
    
    # function to release a job from a node
    def release(self, job: Job):
        if job.id in self.current_jobs:
            self.current_jobs.remove(job.id)
            self.used_cpus-=job.cpus
            self.used_gpus-=job.gpus

# function that defines a cluster
@dataclass
class Cluster:
    nodes: Dict[str, Node]
    now = int = 0

    # function to add the nodes
    def add_nodes(self, n: int, gpus_per_node: int, cpus_per_node: int):
        for i in range(n):
            node_id = 0
            self.nodes[node_id] = Node(node_id, gpus_per_node, cpus_per_node)
            node_id+=1

    # function to see the total utilization of the cluster
    def total_util(self):
        cpu_used = sum(n.used_cpus for n in self.nodes.values())
        cpu_total = sum(n.total_cpus for n in self.nodes.values())
        gpu_used = sum(n.used_gpus for n in self.nodes.values())
        gpu_total = sum(n.total_gpus for n in self.nodes.values())
        return (
            (cpu_used / cpu_total * 100) if cpu_total else 0.0,
            (gpu_used / gpu_total * 100) if gpu_total else 0.0
        )