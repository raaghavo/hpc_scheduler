import argparse, json
from .models import Job
from .scheduler import Scheduler
from . import state as st

def cmd_submit(args):
    cluster, jobs = st.load()
    j = Job(args.name, args.minutes, args.cpus, args.gpus, args.priority)
    j.submit_time = cluster.now
    jobs[j.id] = j
    st.save(cluster, jobs)
    print(f"Submitted {j.name} (id={j.id})")

def cmd_squeue(args):
    cluster, jobs = st.load()
    print(f"== Time: {cluster.now} min ==")
    for j in jobs.values():
        print(f"{j.id} {j.state} {j.name} rem={j.remaining}m node={j.assigned_node}")

def cmd_run(args):
    cluster, jobs = st.load()
    if not cluster.nodes:
        cluster.add_nodes(args.nodes, args.cpus_per_node, args.gpus_per_node)
    sched = Scheduler(policy=args.policy)
    for _ in range(args.duration // args.tick):
        sched.try_schedule(cluster, jobs)
        sched.advance_time(cluster, jobs, args.tick)
    st.save(cluster, jobs)
    print(f"Ran {args.duration} minutes")

def cmd_load_samples(args):
    cluster, jobs = st.load()
    with open(args.path) as f:
        items = json.load(f)
    for it in items:
        j = Job(it["name"], it["minutes"], it["cpus"], it.get("gpus", 0), it.get("priority", 0))
        j.submit_time = cluster.now
        jobs[j.id] = j
    st.save(cluster, jobs)
    print(f"Loaded {len(items)} jobs")

def main():
    p = argparse.ArgumentParser()
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("submit")
    s1.add_argument("--name", required=True)
    s1.add_argument("--minutes", type=int, required=True)
    s1.add_argument("--cpus", type=int, required=True)
    s1.add_argument("--gpus", type=int, default=0)
    s1.add_argument("--priority", type=int, default=0)
    s1.set_defaults(func=cmd_submit)

    s2 = sub.add_parser("squeue")
    s2.set_defaults(func=cmd_squeue)

    s3 = sub.add_parser("run")
    s3.add_argument("--nodes", type=int, default=4)
    s3.add_argument("--cpus-per-node", type=int, default=16)
    s3.add_argument("--gpus-per-node", type=int, default=2)
    s3.add_argument("--policy", choices=["fifo", "priority", "backfill"], default="fifo")
    s3.add_argument("--tick", type=int, default=5)
    s3.add_argument("--duration", type=int, default=60)
    s3.set_defaults(func=cmd_run)

    s4 = sub.add_parser("load-samples")
    s4.add_argument("--path", required=True)
    s4.set_defaults(func=cmd_load_samples)

    args = p.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()
