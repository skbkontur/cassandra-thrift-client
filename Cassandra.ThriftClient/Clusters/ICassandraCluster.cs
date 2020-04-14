using System;
using System.Collections.Generic;

using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Core.Pools;

namespace SkbKontur.Cassandra.ThriftClient.Clusters
{
    public interface ICassandraCluster : IDisposable
    {
        IClusterConnection RetrieveClusterConnection();
        IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName);
        IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName);
        ITimeBasedColumnFamilyConnection RetrieveTimeBasedColumnFamilyConnection(string keyspace, string columnFamily);
        IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
    }
}