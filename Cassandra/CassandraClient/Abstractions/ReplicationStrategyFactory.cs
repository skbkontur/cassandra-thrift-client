using System;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class ReplicationStrategyFactory : IReplicationStrategyFactory
    {
        public IReplicationStrategy Create(string strategyName, Dictionary<string, string> strategyOptions)
        {
            if (strategyOptions == null)
            {
                throw new InvalidOperationException("Strategy options can't be null");
            }

            if(strategyName == ReplicaPlacementStrategy.Local.ToStringValue())
            {
                return LocalReplicationStrategy.Create();
            }

            if (strategyName == ReplicaPlacementStrategy.Simple.ToStringValue())
            {
                if (!strategyOptions.ContainsKey(SimpleReplicationStrategy.ReplicationFactorKey))
                    throw new InvalidOperationException(string.Format("Replication factor should be specified for strategy {0}", strategyName));

                return SimpleReplicationStrategy.Create(int.Parse(strategyOptions[SimpleReplicationStrategy.ReplicationFactorKey]));
            }

            if (strategyName == ReplicaPlacementStrategy.NetworkTopology.ToStringValue())
            {
                var dataCenterReplicationFactors = strategyOptions.Select(x => new DataCenterReplicationFactor {DataCenterName = x.Key, ReplicationFactor = int.Parse(x.Value)}).ToArray();
                return NetworkTopologyReplicationStrategy.Create(dataCenterReplicationFactors);
            }

            throw new NotImplementedException(string.Format("Strategy {0} is not implemented", strategyName));
        }
    }
}