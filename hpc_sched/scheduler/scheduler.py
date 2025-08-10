from __future__ import annotations
from typing import List, Dict, Optional
from .models import Job, Node, Cluster


class Scheduler:
    def __init__(self, policy: str = "fifo"):
        self.policy = policy.lower()

    def try_schedule(self, cluster: Cluster, jobs: Dict[str, Job]):
        pending = [j for j in jobs.values() if j.state == "PENDING"]
        if not pending or not cluster.nodes:
            return

        if self.policy == "fifo":
            self._schedule_fifo(cluster, jobs, pending)
        elif self.policy == "priority":
            self._schedule_priority(cluster, jobs, pending)
        elif self.policy == "backfill":
            self._schedule_backfill(cluster, jobs, pending)
        else:
            # default to fifo if unknown
            self._schedule_fifo(cluster, jobs, pending)

    # Advance simulation time and complete jobs that finish.
    def advance_time(self, cluster: Cluster, jobs: Dict[str, Job], minutes: int):
        cluster.now += minutes
        for job in list(jobs.values()):
            if job.state == "RUNNING":
                job.remaining -= minutes
                if job.remaining <= 0:
                    job.state = "DONE"
                    job.end_time = cluster.now
                    node = cluster.nodes[job.assigned_node]
                    node.release(job)
                    job.assigned_node = None

    # policies
    def _schedule_fifo(self, cluster: Cluster, jobs: Dict[str, Job], pending: List[Job]):
        ordered = sorted(pending, key=lambda j: j.submit_time)
        self._greedy_place_now(cluster, jobs, ordered)

    def _schedule_priority(self, cluster: Cluster, jobs: Dict[str, Job], pending: List[Job]):
        # higher priority first, then FIFO by submit_time
        ordered = sorted(pending, key=lambda j: (-j.priority, j.submit_time))
        self._greedy_place_now(cluster, jobs, ordered)

    def _schedule_backfill(self, cluster: Cluster, jobs: Dict[str, Job], pending: List[Job]):
        """
        here's my backfilling approach:
        - identify the head job (oldest pending).
        - if head fits now, start it (and continue greedy).
        - else compute the head's earliest reservation time on any node.
        - allow other jobs to run only if they can fit NOW and will finish
          before the head's reservation time (i.e., job.minutes <= window).
        """
        # 1) head job = earliest submitted
        head = min(pending, key=lambda j: j.submit_time)

        # 2) ff head fits right now, start it immediately
        if self._try_place_one(cluster, head):
            head.state = "RUNNING"
            head.start_time = cluster.now
            # After placing head, greedily place the rest FIFO
            others = [j for j in pending if j is not head]
            self._greedy_place_now(cluster, jobs, sorted(others, key=lambda j: j.submit_time))
            return

        # 3) compute reservation window for head (time until some node can host it)
        reserve_delta = self._time_until_head_can_fit(cluster, jobs, head)

        # if head is impossible to ever fit on any node (requests exceed any node's capacity),
        # don't block: just place others greedily FIFO (or you could log a warning).
        if reserve_delta is None:
            others = [j for j in pending if j is not head]
            self._greedy_place_now(cluster, jobs, sorted(others, key=lambda j: j.submit_time))
            return

        # 4) backfill: choose other jobs that fit now and will finish before reservation
        backfill_window = reserve_delta  # minutes
        others = [j for j in pending if j is not head]

        # shortest jobs first tends to increase the chance they finish before the head starts
        candidates = sorted(others, key=lambda j: (j.minutes, j.submit_time))

        for job in candidates:
            if job.minutes <= backfill_window:
                self._try_place_one(cluster, job)  # place if it fits now

        # head remains pending; on future calls (after time advances and resources free),
        # it will be scheduled as soon as it actually fits.

    # helpers
    # Greedy pack: try to place each job on the first node that fits right now
    def _greedy_place_now(self, cluster: Cluster, jobs: Dict[str, Job], ordered_jobs: List[Job]):
        for job in ordered_jobs:
            self._try_place_one(cluster, job)

    # try to assign job to any node that can fit it now.
    def _try_place_one(self, cluster: Cluster, job: Job) -> bool:
        if job.state != "PENDING":
            return False
        for node in cluster.nodes.values():
            if node.can_fit(job):
                node.assign(job)
                job.state = "RUNNING"
                job.start_time = cluster.now
                job.assigned_node = node.id
                return True
        return False

    def _time_until_head_can_fit(self, cluster: Cluster, jobs: Dict[str, Job], head: Job) -> Optional[int]:
        """
        compute the minimal time (in minutes from now) after which at least one node
        will have enough free resources to host the head job, assuming currently
        running jobs on that node finish at their 'remaining' times and nothing else
        is scheduled in the meantime.

        returns:
            0 if it fits now,
            positive integer minutes if it will fit later,
            None if it can never fit on any node (exceeds per-node capacity).
        """
        best: Optional[int] = None

        for node in cluster.nodes.values():
            # if the head request exceeds the node's *total* capacity, skip node entirely
            if head.cpus > node.total_cpus or head.gpus > node.total_gpus:
                continue

            # if it fits now on this node, reservation is immediate
            if node.can_fit(head):
                return 0

            # simulate resource freeing as running jobs on this node finish
            avail_cpus = node.total_cpus - node.used_cpus
            avail_gpus = node.total_gpus - node.used_gpus

            # gather running jobs and their remaining times on this node
            events = []
            for jid in list(node.running_jobs):
                j = jobs[jid]
                # when this job completes, the resources it used become available
                events.append((j.remaining, j.cpus, j.gpus))
            # sort by soonest completion
            events.sort(key=lambda x: x[0])

            freed_cpus = 0
            freed_gpus = 0
            t_candidate = None

            for t, c, g in events:
                freed_cpus += c
                freed_gpus += g
                if (avail_cpus + freed_cpus) >= head.cpus and (avail_gpus + freed_gpus) >= head.gpus:
                    t_candidate = t  # earliest time (from now) this node can fit the head
                    break

            if t_candidate is not None:
                if best is None or t_candidate < best:
                    best = t_candidate

        return best
