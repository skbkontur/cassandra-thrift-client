using System.Collections.Generic;
using System.Globalization;

using SkbKontur.Cassandra.ThriftClient.Scheme;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class SimpleReplicationStrategy : IReplicationStrategy
    {
        private SimpleReplicationStrategy()
        {
        }

        public const string ReplicationFactorKey = "replication_factor";

        public int ReplicationFactor { get; private set; }

        public string Name => ReplicaPlacementStrategy.Simple.ToStringValue();

        public Dictionary<string, string> StrategyOptions => new Dictionary<string, string> {{ReplicationFactorKey, ReplicationFactor.ToString(CultureInfo.InvariantCulture)}};

        public static SimpleReplicationStrategy Create(int replicationFactor)
        {
            return new SimpleReplicationStrategy
                {
                    ReplicationFactor = replicationFactor
                };
        }
    }
}