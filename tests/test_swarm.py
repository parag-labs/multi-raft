from flotilla import Cluster


def _swarm(n_groups: int, nodes=("n1", "n2", "n3")):
    c = Cluster(list(nodes))
    for g in range(n_groups):
        c.add_group(f"g{g}", list(nodes))
    c.run_once()
    return c


def test_every_group_agrees_on_one_leader():
    c = _swarm(50)
    leaders = c.leaders()
    assert len(leaders) == 50
    for gid, who in leaders.items():
        assert len(who) == 1, f"group {gid} disagreed on its leader: {who}"


def test_leader_is_the_min_id_member():
    c = _swarm(10)
    for gid, who in c.leaders().items():
        assert who == {"n1"}  # min of n1,n2,n3


def test_batching_beats_one_rpc_per_group():
    # 200 groups across 3 nodes. Without batching the leader would emit
    # 200 * 2 = 400 separate RPCs; batching collapses each node's outbound to
    # at most (peers) envelopes, so envelopes << messages.
    c = _swarm(200)
    assert c.total_messages() >= 400
    assert c.total_envelopes() <= 6  # <= 3 nodes * 2 peers
    assert c.total_envelopes() < c.total_messages() / 10


def test_scales_to_thousands_of_groups():
    c = _swarm(2000)
    leaders = c.leaders()
    assert len(leaders) == 2000
    assert all(len(w) == 1 for w in leaders.values())
    # Envelope count stays bounded by peer pairs even at 2000 groups.
    assert c.total_envelopes() <= 6
