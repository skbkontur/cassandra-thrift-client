using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;

using SkbKontur.Cassandra.ThriftClient.Schema;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class NetworkTopologyReplicationStrategy : IReplicationStrategy
    {
        private NetworkTopologyReplicationStrategy()
        {
        }

        public DataCenterReplicationFactor[] DataCenterReplicationFactors { get; private set; }

        public string Name => ReplicaPlacementStrategy.NetworkTopology.ToStringValue();

        public Dictionary<string, string> StrategyOptions { get { return DataCenterReplicationFactors.ToDictionary(x => x.DataCenterName, x => x.ReplicationFactor.ToString(CultureInfo.InvariantCulture)); } }

        public static NetworkTopologyReplicationStrategy Create(DataCenterReplicationFactor[] dataCenterReplicationFactors)
        {
            if (dataCenterReplicationFactors == null || dataCenterReplicationFactors.Length == 0)
                throw new InvalidOperationException("Data center replication factors should be specified");

            return new NetworkTopologyReplicationStrategy
                {
                    DataCenterReplicationFactors = dataCenterReplicationFactors
                };
        }
    }
}