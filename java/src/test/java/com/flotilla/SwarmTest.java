package com.flotilla;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SwarmTest {

    private static final List<String> NODES = List.of("n1", "n2", "n3");

    private static Cluster swarm(int nGroups) {
        Cluster c = new Cluster(NODES);
        for (int g = 0; g < nGroups; g++) c.addGroup("g" + g, NODES);
        c.runOnce();
        return c;
    }

    @Test
    void everyGroupAgreesOnOneLeader() {
        Cluster c = swarm(50);
        Map<String, Set<String>> leaders = c.leaders();
        assertEquals(50, leaders.size());
        for (Map.Entry<String, Set<String>> e : leaders.entrySet()) {
            assertEquals(1, e.getValue().size(), "group " + e.getKey() + " disagreed on its leader");
        }
    }

    @Test
    void leaderIsTheMinIdMember() {
        Cluster c = swarm(10);
        for (Set<String> who : c.leaders().values()) {
            assertEquals(Set.of("n1"), who); // min of n1,n2,n3
        }
    }

    @Test
    void batchingBeatsOneRpcPerGroup() {
        // 200 groups across 3 nodes. Without batching the leader would emit
        // 200 * 2 = 400 separate RPCs; batching collapses each node's outbound to
        // at most (peers) envelopes, so envelopes << messages.
        Cluster c = swarm(200);
        assertTrue(c.totalMessages() >= 400);
        assertTrue(c.totalEnvelopes() <= 6); // <= 3 nodes * 2 peers
        assertTrue(c.totalEnvelopes() < c.totalMessages() / 10.0);
    }

    @Test
    void scalesToThousandsOfGroups() {
        Cluster c = swarm(2000);
        Map<String, Set<String>> leaders = c.leaders();
        assertEquals(2000, leaders.size());
        for (Set<String> w : leaders.values()) assertEquals(1, w.size());
        // Envelope count stays bounded by peer pairs even at 2000 groups.
        assertTrue(c.totalEnvelopes() <= 6);
    }
}
