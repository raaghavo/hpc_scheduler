import json
from typing import Dict
from .models import Job, Node, Cluster


STATE_FILE = ".slurm_state.json"

# saves the state of the cluster
def save(cluster: Cluster, jobs: Dict[str, Job], path: str = STATE_FILE):
    data = {
        "cluster": {
            "now": cluster.now,
            "nodes": {
                nid: {
                    "total_cpus": n.total_cpus,
                    "total_gpus": n.total_gpus,
                    "used_cpus": n.used_cpus,
                    "used_gpus": n.used_gpus,
                    "running_jobs": n.running_jobs
                } for nid, n in cluster.nodes.items()
            }
        },
        "jobs": {
            jid: vars(j) for jid, j in jobs.items()
        }
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# loads the saved state of a cluster
def load(path: str = STATE_FILE):

    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        return Cluster(), {}
    
    cluster = Cluster()
    cluster.now = data["cluster"]["now"]

    for nid, nd in data["cluster"]["nodes"].items():
        cluster.nodes[nid] = Node(
            id=nid,
            total_cpus=nd["total_cpus"],
            total_gpus=nd["total_gpus"],
            used_cpus=nd["used_cpus"],
            used_gpus=nd["used_gpus"],
            running_jobs=nd["running_jobs"]
        )
    jobs: Dict[str, Job] = {}

    for jid, jd in data["jobs"].items():
        j = Job(
            name=jd["name"],
            minutes=jd["minutes"],
            cpus=jd["cpus"],
            gpus=jd["gpus"],
            priority=jd["priority"],
            id=jd["id"]
        )
        j.state = jd["state"]
        j.remaining = jd["remaining"]
        j.assigned_node = jd["assigned_node"]
        j.submit_time = jd["submit_time"]
        j.start_time = jd["start_time"]
        j.end_time = jd["end_time"]
        jobs[j.id] = j

    return cluster, jobs