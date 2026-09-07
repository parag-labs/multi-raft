using Xunit;

namespace Flotilla.Tests;

public class SwarmTests
{
    private static Cluster Swarm(int nGroups, string[]? nodes = null)
    {
        nodes ??= new[] { "n1", "n2", "n3" };
        var c = new Cluster(nodes);
        for (int g = 0; g < nGroups; g++)
            c.AddGroup($"g{g}", nodes.ToList());
        c.RunOnce();
        return c;
    }

    [Fact]
    public void EveryGroupAgreesOnOneLeader()
    {
        var c = Swarm(50);
        var leaders = c.Leaders();
        Assert.Equal(50, leaders.Count);
        foreach (var (gid, who) in leaders)
            Assert.True(who.Count == 1, $"group {gid} disagreed on its leader");
    }

    [Fact]
    public void LeaderIsTheMinIdMember()
    {
        var c = Swarm(10);
        foreach (var (_, who) in c.Leaders())
            Assert.Equal(new HashSet<string> { "n1" }, who); // min of n1,n2,n3
    }

    [Fact]
    public void BatchingBeatsOneRpcPerGroup()
    {
        // 200 groups across 3 nodes. Without batching the leader would emit
        // 200 * 2 = 400 separate RPCs; batching collapses each node's outbound to
        // at most (peers) envelopes, so envelopes << messages.
        var c = Swarm(200);
        Assert.True(c.TotalMessages() >= 400);
        Assert.True(c.TotalEnvelopes() <= 6); // <= 3 nodes * 2 peers
        Assert.True(c.TotalEnvelopes() < c.TotalMessages() / 10.0);
    }

    [Fact]
    public void ScalesToThousandsOfGroups()
    {
        var c = Swarm(2000);
        var leaders = c.Leaders();
        Assert.Equal(2000, leaders.Count);
        Assert.All(leaders.Values, w => Assert.Single(w));
        // Envelope count stays bounded by peer pairs even at 2000 groups.
        Assert.True(c.TotalEnvelopes() <= 6);
    }
}
