# hpc scheduler

A lightweight, standard-library Python hpc scheduler simulator to learn and demo **HPC scheduling** concepts (like SLURM) on your laptop.  
It simulates **jobs**, a **cluster** of nodes (CPUs/GPUs), and multiple **scheduling policies**:

- **FIFO** (First-In First-Out)
- **Priority** (optional, if you enable it)
- **Conservative Backfilling** 


## How to Run
After you have cloned the repo, do the following:

Load the sample workloads

rm -f .slurm_state.json
python3 -m hpc_sched.scheduler.cli load-samples --path ./hpc_sched/data/sample.json

Use python3 -m hpc_sched.scheduler.cli -h to view the options to use this scheduler
