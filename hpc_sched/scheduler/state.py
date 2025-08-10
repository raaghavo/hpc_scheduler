import json
from typing import Dict
from .models import Job, Node, Cluster

STATE_FILE = ".slurm_state.json"

# save the state
def save(cluster: Cluster, jobs: Dict[str, Job], path: str = STATE_FILE):
    """Persist cluster + jobs as a stable JSON shape."""
    data = {
        "cluster": {
            "now": cluster.now,
            "nodes": {
                nid: {
                    "total_cpus": n.total_cpus,
                    "total_gpus": n.total_gpus,
                    "used_cpus": n.used_cpus,
                    "used_gpus": n.used_gpus,
                    "running_jobs": n.running_jobs,
                }
                for nid, n in cluster.nodes.items()
            },
        },
        "jobs": {
            jid: {
                "name": j.name,
                "minutes": j.minutes,
                "cpus": j.cpus,
                "gpus": j.gpus,
                "priority": j.priority,
                "id": j.id,
                "state": j.state,
                "remaining": j.remaining,
                "assigned_node": j.assigned_node,
                "submit_time": j.submit_time,
                "start_time": j.start_time,
                "end_time": j.end_time,
            }
            for jid, j in jobs.items()
        },
    }
    with open(path, "w") as f:
        json.dump(data, f, indent=2)

# load the state
def load(path: str = STATE_FILE):
    try:
        with open(path) as f:
            data = json.load(f)
    except FileNotFoundError:
        return Cluster(), {}

    # Cluster
    cluster_data = data.get("cluster", {})
    cluster = Cluster()
    cluster.now = cluster_data.get("now", 0)
    for nid, nd in cluster_data.get("nodes", {}).items():
        cluster.nodes[nid] = Node(
            id=nid,
            total_cpus=nd.get("total_cpus", 0),
            total_gpus=nd.get("total_gpus", 0),
            used_cpus=nd.get("used_cpus", 0),
            used_gpus=nd.get("used_gpus", 0),
            running_jobs=nd.get("running_jobs", []),
        )

    # jobs
    jobs: Dict[str, Job] = {}
    for jid, jd in data.get("jobs", {}).items():
        j = Job(
            name=jd["name"],
            minutes=jd["minutes"],
            cpus=jd["cpus"],
            gpus=jd.get("gpus", 0),
            priority=jd.get("priority", 0),
            id=jd.get("id", jid),
        )
        #defaults
        j.state = jd.get("state", "PENDING")
        j.remaining = jd.get("remaining", j.minutes)
        j.assigned_node = jd.get("assigned_node", None)
        j.submit_time = jd.get("submit_time", 0)
        j.start_time = jd.get("start_time", None)
        j.end_time = jd.get("end_time", None)
        jobs[j.id] = j

    return cluster, jobs
