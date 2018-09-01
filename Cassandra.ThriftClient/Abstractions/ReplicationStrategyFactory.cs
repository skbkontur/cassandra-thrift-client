using System;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal class ReplicationStrategyFactory : IReplicationStrategyFactory
    {
        public IReplicationStrategy Create(KsDef ksDef)
        {
            if (ksDef.Strategy_options == null)
                throw new InvalidOperationException($"ksDef.Strategy_options == null for: {ksDef}");

            if (ksDef.Strategy_class == ReplicaPlacementStrategy.Simple.ToStringValue())
            {
                if (!ksDef.Strategy_options.ContainsKey(SimpleReplicationStrategy.ReplicationFactorKey))
                    throw new InvalidOperationException($"Replication factor should be specified for strategy {ksDef.Strategy_class} in: {ksDef}");

                return SimpleReplicationStrategy.Create(int.Parse(ksDef.Strategy_options[SimpleReplicationStrategy.ReplicationFactorKey]));
            }

            if (ksDef.Strategy_class == ReplicaPlacementStrategy.NetworkTopology.ToStringValue())
            {
                var dataCenterReplicationFactors = ksDef.Strategy_options.Select(x => new DataCenterReplicationFactor(x.Key, int.Parse(x.Value))).ToArray();
                return NetworkTopologyReplicationStrategy.Create(dataCenterReplicationFactors);
            }

            throw new InvalidOperationException($"Strategy {ksDef.Strategy_class} is not implemented for: {ksDef}");
        }

        public static readonly ReplicationStrategyFactory FactoryInstance = new ReplicationStrategyFactory();
    }
}