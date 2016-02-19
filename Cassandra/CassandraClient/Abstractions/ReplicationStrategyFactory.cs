using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal class ReplicationStrategyFactory : IReplicationStrategyFactory
    {
        public static readonly ReplicationStrategyFactory FactoryInstance = new ReplicationStrategyFactory();

        public IReplicationStrategy Create(string strategyName, Dictionary<string, string> strategyOptions)
        {
            if (strategyOptions == null)
            {
                throw new InvalidOperationException("Strategy options can't be null");
            }

            if (strategyName == ReplicaPlacementStrategy.Simple.ToStringValue())
            {
                if (!strategyOptions.ContainsKey(SimpleReplicationStrategy.ReplicationFactorKey))
                    throw new InvalidOperationException(string.Format("Replication factor should be specified for strategy {0}", strategyName));

                return SimpleReplicationStrategy.Create(int.Parse(strategyOptions[SimpleReplicationStrategy.ReplicationFactorKey]));
            }

            if (strategyName == ReplicaPlacementStrategy.NetworkTopology.ToStringValue())
            {
                var dataCenterReplicationFactors = strategyOptions.Select(x => new DataCenterReplicationFactor(x.Key, int.Parse(x.Value))).ToArray();
                return NetworkTopologyReplicationStrategy.Create(dataCenterReplicationFactors);
            }

            throw new InvalidOperationException(string.Format("Strategy {0} is not implemented", strategyName));
        }
    }
}