"""A tiny in-memory cluster that wires schedulers together and pumps envelopes.

Used by the tests and the benchmark to show a swarm of groups converging on their
leaders while the RPC count stays proportional to the number of *peer pairs*, not
the number of groups.
"""

from __future__ import annotations

from .scheduler import GroupReplica, Scheduler


class Cluster:
    def __init__(self, node_ids: list[str]) -> None:
        self.schedulers = {n: Scheduler(n) for n in node_ids}

    def add_group(self, group_id: str, members: list[str]) -> None:
        for n in members:
            self.schedulers[n].add(GroupReplica(group_id, n, list(members)))

    def run_once(self) -> None:
        """Bootstrap every node, then deliver the resulting envelopes."""
        pending: dict[str, list[dict]] = {}
        for node, sched in self.schedulers.items():
            for dst, envelope in sched.bootstrap().items():
                pending.setdefault(dst, []).extend(envelope)
        for dst, envelope in pending.items():
            self.schedulers[dst].deliver(envelope)

    def total_messages(self) -> int:
        return sum(s.messages_sent for s in self.schedulers.values())

    def total_envelopes(self) -> int:
        return sum(s.envelopes_sent for s in self.schedulers.values())

    def leaders(self) -> dict[str, set[str]]:
        """group_id -> set of leaders each replica believes in (should be size 1)."""
        seen: dict[str, set[str]] = {}
        for sched in self.schedulers.values():
            for gid, r in sched.replicas.items():
                if r.leader is not None:
                    seen.setdefault(gid, set()).add(r.leader)
        return seen
