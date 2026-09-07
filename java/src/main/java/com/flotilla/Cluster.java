// A tiny in-memory cluster that wires schedulers together and pumps envelopes.
//
// Used by the tests and the benchmark to show a fleet of groups converging on their
// leaders while the RPC count stays proportional to the number of peer pairs, not
// the number of groups.

package com.flotilla;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class Cluster {

    private final Map<String, Scheduler> schedulers = new LinkedHashMap<>();

    public Cluster(List<String> nodeIds) {
        for (String n : nodeIds) schedulers.put(n, new Scheduler(n));
    }

    public void addGroup(String groupId, List<String> members) {
        for (String n : members) {
            schedulers.get(n).add(new GroupReplica(groupId, n, members));
        }
    }

    /** Bootstrap every node, then deliver the resulting envelopes. */
    public void runOnce() {
        Map<String, List<GroupMessage>> pending = new LinkedHashMap<>();
        for (Scheduler sched : schedulers.values()) {
            for (Map.Entry<String, List<GroupMessage>> e : sched.bootstrap().entrySet()) {
                pending.computeIfAbsent(e.getKey(), k -> new ArrayList<>()).addAll(e.getValue());
            }
        }
        for (Map.Entry<String, List<GroupMessage>> e : pending.entrySet()) {
            schedulers.get(e.getKey()).deliver(e.getValue());
        }
    }

    public int totalMessages() {
        int t = 0;
        for (Scheduler s : schedulers.values()) t += s.messagesSent;
        return t;
    }

    public int totalEnvelopes() {
        int t = 0;
        for (Scheduler s : schedulers.values()) t += s.envelopesSent;
        return t;
    }

    /** group_id -> set of leaders each replica believes in (should be size 1). */
    public Map<String, Set<String>> leaders() {
        Map<String, Set<String>> seen = new LinkedHashMap<>();
        for (Scheduler sched : schedulers.values()) {
            for (Map.Entry<String, GroupReplica> e : sched.replicas.entrySet()) {
                GroupReplica r = e.getValue();
                if (r.leader != null) {
                    seen.computeIfAbsent(e.getKey(), k -> new HashSet<>()).add(r.leader);
                }
            }
        }
        return seen;
    }
}
