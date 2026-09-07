// A tiny in-memory cluster that wires schedulers together and pumps envelopes.
//
// Used by the tests and the benchmark to show a fleet of groups converging on their
// leaders while the RPC count stays proportional to the number of peer pairs, not
// the number of groups.

namespace Flotilla;

public sealed class Cluster
{
    public Dictionary<string, Scheduler> Schedulers { get; } = new();

    public Cluster(IEnumerable<string> nodeIds)
    {
        foreach (var n in nodeIds) Schedulers[n] = new Scheduler(n);
    }

    public void AddGroup(string groupId, List<string> members)
    {
        foreach (var n in members)
            Schedulers[n].Add(new GroupReplica(groupId, n, members));
    }

    /// <summary>Bootstrap every node, then deliver the resulting envelopes.</summary>
    public void RunOnce()
    {
        var pending = new Dictionary<string, List<GroupMessage>>();
        foreach (var sched in Schedulers.Values)
        {
            foreach (var (dst, envelope) in sched.Bootstrap())
            {
                if (!pending.TryGetValue(dst, out var list))
                {
                    list = new List<GroupMessage>();
                    pending[dst] = list;
                }
                list.AddRange(envelope);
            }
        }
        foreach (var (dst, envelope) in pending)
            Schedulers[dst].Deliver(envelope);
    }

    public int TotalMessages() => Schedulers.Values.Sum(s => s.MessagesSent);

    public int TotalEnvelopes() => Schedulers.Values.Sum(s => s.EnvelopesSent);

    /// <summary>group_id -> set of leaders each replica believes in (should be size 1).</summary>
    public Dictionary<string, HashSet<string>> Leaders()
    {
        var seen = new Dictionary<string, HashSet<string>>();
        foreach (var sched in Schedulers.Values)
        {
            foreach (var (gid, r) in sched.Replicas)
            {
                if (r.Leader is not null)
                {
                    if (!seen.TryGetValue(gid, out var set))
                    {
                        set = new HashSet<string>();
                        seen[gid] = set;
                    }
                    set.Add(r.Leader);
                }
            }
        }
        return seen;
    }
}
