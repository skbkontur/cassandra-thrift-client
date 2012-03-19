using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;

namespace SKBKontur.Cassandra.CassandraClient.Clusters
{
    public interface ICassandraCluster : IDisposable
    {
        IClusterConnection RetrieveClusterConnection();
        IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName);
        IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
        void CheckConnections();
    }
}