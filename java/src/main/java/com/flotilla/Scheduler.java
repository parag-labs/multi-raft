// The multiplexing layer: many consensus groups sharing one loop and one wire.
//
// The expensive parts of running thousands of Raft groups on a box aren't the Raft
// state machines - those are cheap. It's (1) a thread per group, and (2) a separate
// RPC for every group even when a hundred of them are talking to the same physical
// peer. This file is about avoiding both: one scheduler drives every local group
// replica in a single tick, and outbound messages to the same peer are coalesced
// into one envelope.
//
// The actual consensus lives in the sibling coracle project. Flotilla is the layer
// that lets you run a fleet of them.

package com.flotilla;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** One inner message routed to one group on one node. */
record GroupMessage(String kind, String group, int term, String leader) {}

/** A destination-tagged message emitted by a replica. */
record Addressed(String dst, GroupMessage msg) {}

/** One node's replica of one consensus group.
 *
 * Election here is intentionally trivial - the node with the smallest id in the
 * group is the leader - because the point of this repo is the scheduling and
 * batching layer, not re-deriving Raft. Swap this state machine for a real coracle
 * node and the scheduler/batcher are unchanged. */
class GroupReplica {

    final String groupId;
    final String localId;
    final List<String> peers;
    String role = "follower";
    String leader = null;
    int term = 0;

    GroupReplica(String groupId, String localId, List<String> peers) {
        this.groupId = groupId;
        this.localId = localId;
        this.peers = new ArrayList<>(peers);
    }

    /** Called once. If we're the designated leader, announce it to peers. */
    List<Addressed> bootstrap() {
        List<Addressed> outbound = new ArrayList<>();
        if (localId.equals(Collections.min(peers))) {
            role = "leader";
            leader = localId;
            term = 1;
            for (String p : peers) {
                if (!p.equals(localId)) {
                    outbound.add(new Addressed(p, new GroupMessage("leader", groupId, 1, localId)));
                }
            }
        }
        return outbound;
    }

    List<Addressed> handle(GroupMessage msg) {
        if (msg.kind().equals("leader") && msg.term() >= term) {
            term = msg.term();
            leader = msg.leader();
            role = "follower";
        }
        return new ArrayList<>();
    }
}

/** One physical node's executor. Owns all of that node's group replicas and drives
 * them without a thread per group. */
public class Scheduler {

    final String nodeId;
    final Map<String, GroupReplica> replicas = new LinkedHashMap<>();
    // Metrics so tests (and humans) can see the batching pay off.
    int envelopesSent = 0;
    int messagesSent = 0;

    public Scheduler(String nodeId) {
        this.nodeId = nodeId;
    }

    void add(GroupReplica replica) {
        replicas.put(replica.groupId, replica);
    }

    Map<String, List<GroupMessage>> bootstrap() {
        List<Addressed> outbound = new ArrayList<>();
        for (GroupReplica r : replicas.values()) outbound.addAll(r.bootstrap());
        return batch(outbound);
    }

    /** Coalesce per-group messages headed to the same peer into one envelope.
     * 1000 groups each sending a heartbeat to peer X become a single envelope
     * carrying 1000 inner messages, not 1000 RPCs. */
    private Map<String, List<GroupMessage>> batch(List<Addressed> outbound) {
        Map<String, List<GroupMessage>> byDst = new LinkedHashMap<>();
        for (Addressed a : outbound) {
            byDst.computeIfAbsent(a.dst(), k -> new ArrayList<>()).add(a.msg());
        }
        messagesSent += outbound.size();
        envelopesSent += byDst.size();
        return byDst;
    }

    /** Unpack an envelope and fan its inner messages out to the right group
     * replicas, collecting any replies (already re-batched). */
    Map<String, List<GroupMessage>> deliver(List<GroupMessage> envelope) {
        List<Addressed> replies = new ArrayList<>();
        for (GroupMessage msg : envelope) {
            GroupReplica replica = replicas.get(msg.group());
            if (replica != null) replies.addAll(replica.handle(msg));
        }
        return batch(replies);
    }
}
