using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class LocalReplicationStrategy : IReplicationStrategy
    {
        public string Name { get { return ReplicaPlacementStrategy.Local.ToStringValue(); } }
        public Dictionary<string, string> StrategyOptions { get { return new Dictionary<string, string>();} }

        public static LocalReplicationStrategy Create()
        {
            return new LocalReplicationStrategy();
        }
    }
}