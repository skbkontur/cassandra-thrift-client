using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    public enum ReplicaPlacementStrategy
    {
        [StringValue("org.apache.cassandra.locator.SimpleStrategy")]
        Simple,

        [StringValue("org.apache.cassandra.locator.NetworkTopologyStrategy")]
        NetworkTopology
    }
}