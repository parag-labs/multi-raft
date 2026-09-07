"""The multiplexing layer: many consensus groups sharing one loop and one wire.

The expensive parts of running thousands of Raft groups on a box aren't the Raft
state machines - those are cheap. It's (1) a thread per group, and (2) a separate
RPC for every group even when a hundred of them are talking to the same physical
peer. This module is about avoiding both: one scheduler drives every local group
replica in a single tick, and outbound messages to the same peer are coalesced
into one envelope.

The actual consensus (real elections, log replication, the current-term commit
rule) is deliberately *not* reimplemented here - that lives in the sibling
`mini-raft` project. multi-raft is the layer that lets you run a swarm of them.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class GroupReplica:
    """One node's replica of one consensus group.

    Election here is intentionally trivial - the node with the smallest id in the
    group is the leader - because the point of this repo is the scheduling and
    batching layer, not re-deriving Raft. Swap this state machine for a real
    mini-raft node and the scheduler/batcher below are unchanged.
    """

    group_id: str
    local_id: str
    peers: list[str]
    role: str = "follower"
    leader: str | None = None
    term: int = 0

    def bootstrap(self) -> list[tuple[str, dict]]:
        """Called once. If we're the designated leader, announce it to peers."""
        if self.local_id == min(self.peers):
            self.role = "leader"
            self.leader = self.local_id
            self.term = 1
            return [
                (p, {"kind": "leader", "group": self.group_id, "term": 1, "leader": self.local_id})
                for p in self.peers
                if p != self.local_id
            ]
        return []

    def handle(self, msg: dict) -> list[tuple[str, dict]]:
        if msg["kind"] == "leader" and msg["term"] >= self.term:
            self.term = msg["term"]
            self.leader = msg["leader"]
            self.role = "follower"
        return []


@dataclass
class Scheduler:
    """One physical node's executor. Owns all of that node's group replicas and
    drives them without a thread per group."""

    node_id: str
    replicas: dict[str, GroupReplica] = field(default_factory=dict)
    # Metrics so tests (and humans) can see the batching pay off.
    envelopes_sent: int = 0
    messages_sent: int = 0

    def add(self, replica: GroupReplica) -> None:
        self.replicas[replica.group_id] = replica

    def bootstrap(self) -> dict[str, list[dict]]:
        outbound: list[tuple[str, dict]] = []
        for r in self.replicas.values():
            outbound.extend(r.bootstrap())
        return self._batch(outbound)

    def _batch(self, outbound: list[tuple[str, dict]]) -> dict[str, list[dict]]:
        """Coalesce per-group messages headed to the same peer into one envelope.

        1000 groups each sending a heartbeat to peer X become a single envelope
        carrying 1000 inner messages, not 1000 RPCs."""
        by_dst: dict[str, list[dict]] = defaultdict(list)
        for dst, msg in outbound:
            by_dst[dst].append(msg)
        self.messages_sent += len(outbound)
        self.envelopes_sent += len(by_dst)
        return dict(by_dst)

    def deliver(self, envelope: list[dict]) -> dict[str, list[dict]]:
        """Unpack an envelope and fan its inner messages out to the right group
        replicas, collecting any replies (already re-batched)."""
        replies: list[tuple[str, dict]] = []
        for msg in envelope:
            replica = self.replicas.get(msg["group"])
            if replica is not None:
                replies.extend(replica.handle(msg))
        return self._batch(replies)
