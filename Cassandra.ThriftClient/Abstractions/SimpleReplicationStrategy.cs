using System.Collections.Generic;
using System.Globalization;

using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class SimpleReplicationStrategy : IReplicationStrategy
    {
        public const string ReplicationFactorKey = "replication_factor";

        private SimpleReplicationStrategy()
        {
        }

        public int ReplicationFactor { get; private set; }

        public string Name { get { return ReplicaPlacementStrategy.Simple.ToStringValue(); } }

        public Dictionary<string, string> StrategyOptions
        {
            get
            {
                return new Dictionary<string, string> { { ReplicationFactorKey, ReplicationFactor.ToString(CultureInfo.InvariantCulture) } };
            }
        }

        public static SimpleReplicationStrategy Create(int replicationFactor)
        {
            return new SimpleReplicationStrategy
                {
                    ReplicationFactor = replicationFactor
                };
        }
    }
}