// The multiplexing layer: many consensus groups sharing one loop and one wire.
//
// The expensive parts of running thousands of Raft groups on a box aren't the Raft
// state machines - those are cheap. It's (1) a thread per group, and (2) a separate
// RPC for every group even when a hundred of them are talking to the same physical
// peer. This file is about avoiding both: one scheduler drives every local group
// replica in a single tick, and outbound messages to the same peer are coalesced
// into one envelope.
//
// The actual consensus (real elections, log replication, the current-term commit
// rule) is deliberately not reimplemented here - that lives in the sibling coracle
// project. Flotilla is the layer that lets you run a fleet of them.

namespace Flotilla;

/// <summary>A single inner message routed to one group on one node.</summary>
public sealed class GroupMessage
{
    public required string Kind { get; init; }
    public required string Group { get; init; }
    public int Term { get; init; }
    public string? Leader { get; init; }
}

/// <summary>One node's replica of one consensus group.
///
/// Election here is intentionally trivial - the node with the smallest id in the
/// group is the leader - because the point of this repo is the scheduling and
/// batching layer, not re-deriving Raft. Swap this state machine for a real coracle
/// node and the scheduler/batcher are unchanged.</summary>
public sealed class GroupReplica
{
    public string GroupId { get; }
    public string LocalId { get; }
    public List<string> Peers { get; }
    public string Role { get; private set; } = "follower";
    public string? Leader { get; private set; }
    public int Term { get; private set; }

    public GroupReplica(string groupId, string localId, IEnumerable<string> peers)
    {
        GroupId = groupId;
        LocalId = localId;
        Peers = peers.ToList();
    }

    /// <summary>Called once. If we're the designated leader, announce it to peers.</summary>
    public List<(string dst, GroupMessage msg)> Bootstrap()
    {
        var outbound = new List<(string, GroupMessage)>();
        if (LocalId == Peers.Min())
        {
            Role = "leader";
            Leader = LocalId;
            Term = 1;
            foreach (var p in Peers)
            {
                if (p != LocalId)
                    outbound.Add((p, new GroupMessage
                    {
                        Kind = "leader", Group = GroupId, Term = 1, Leader = LocalId,
                    }));
            }
        }
        return outbound;
    }

    public List<(string dst, GroupMessage msg)> Handle(GroupMessage msg)
    {
        if (msg.Kind == "leader" && msg.Term >= Term)
        {
            Term = msg.Term;
            Leader = msg.Leader;
            Role = "follower";
        }
        return new List<(string, GroupMessage)>();
    }
}

/// <summary>One physical node's executor. Owns all of that node's group replicas
/// and drives them without a thread per group.</summary>
public sealed class Scheduler
{
    public string NodeId { get; }
    public Dictionary<string, GroupReplica> Replicas { get; } = new();
    // Metrics so tests (and humans) can see the batching pay off.
    public int EnvelopesSent { get; private set; }
    public int MessagesSent { get; private set; }

    public Scheduler(string nodeId) => NodeId = nodeId;

    public void Add(GroupReplica replica) => Replicas[replica.GroupId] = replica;

    public Dictionary<string, List<GroupMessage>> Bootstrap()
    {
        var outbound = new List<(string, GroupMessage)>();
        foreach (var r in Replicas.Values) outbound.AddRange(r.Bootstrap());
        return Batch(outbound);
    }

    /// <summary>Coalesce per-group messages headed to the same peer into one
    /// envelope. 1000 groups each sending a heartbeat to peer X become a single
    /// envelope carrying 1000 inner messages, not 1000 RPCs.</summary>
    private Dictionary<string, List<GroupMessage>> Batch(List<(string dst, GroupMessage msg)> outbound)
    {
        var byDst = new Dictionary<string, List<GroupMessage>>();
        foreach (var (dst, msg) in outbound)
        {
            if (!byDst.TryGetValue(dst, out var list))
            {
                list = new List<GroupMessage>();
                byDst[dst] = list;
            }
            list.Add(msg);
        }
        MessagesSent += outbound.Count;
        EnvelopesSent += byDst.Count;
        return byDst;
    }

    /// <summary>Unpack an envelope and fan its inner messages out to the right group
    /// replicas, collecting any replies (already re-batched).</summary>
    public Dictionary<string, List<GroupMessage>> Deliver(List<GroupMessage> envelope)
    {
        var replies = new List<(string, GroupMessage)>();
        foreach (var msg in envelope)
        {
            if (Replicas.TryGetValue(msg.Group, out var replica))
                replies.AddRange(replica.Handle(msg));
        }
        return Batch(replies);
    }
}
