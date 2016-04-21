using System;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Clusters
{
    public interface ICassandraCluster : IDisposable
    {
        IClusterConnection RetrieveClusterConnection();
        IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName);
        IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName);
        IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
        void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null, bool changeExistingKeyspaceMetadata = false);
    }
}