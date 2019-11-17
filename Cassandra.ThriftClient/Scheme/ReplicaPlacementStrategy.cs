using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Scheme
{
    public enum ReplicaPlacementStrategy
    {
        [StringValue("org.apache.cassandra.locator.SimpleStrategy")]
        Simple,

        [StringValue("org.apache.cassandra.locator.NetworkTopologyStrategy")]
        NetworkTopology
    }
}