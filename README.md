# hpc scheduler

A lightweight, standard-library Python hpc scheduler simulator to learn and demo **HPC scheduling** concepts (like SLURM) on your laptop.  
It simulates **jobs**, a **cluster** of nodes (CPUs/GPUs), and multiple **scheduling policies**:

- **FIFO** (First-In First-Out)
- **Priority** (optional, if you enable it)
- **Conservative Backfilling** 

---

## Why this project?

- Practice **scheduling trade-offs** (throughput vs. fairness vs. latency).
- Run locally, reproducibly, and quickly.

---

## Project layout
project_root/
├─ hpc_sched/
│  ├─ __init__.py
│  ├─ data/
│  │  └─ sample.json
│  └─ scheduler/
│     ├─ __init__.py
│     ├─ cli.py          # CLI entrypoint
│     ├─ models.py       # Job, Node, Cluster
│     ├─ scheduler.py    # Policies: fifo, backfill (+ priority hook)
│     └─ state.py        # JSON save/load, tolerant to missing fields
└─ README.md


## How to Run
After you have cloned the repo, do the following:

1. rm -f .slurm_state.json
   
3. python3 -m hpc_sched.scheduler.cli load-samples --path ./hpc_sched/data/sample.json
   
3.python3 -m hpc_sched.scheduler.cli run \
  --nodes 2 --cpus-per-node 8 --gpus-per-node 1 \
  --policy fifo --tick 5 --duration 30

4. python3 -m hpc_sched.scheduler.cli squeue
