using System.Collections.Generic;

using CassandraClient.Connections;
using CassandraClient.Core.Pools;

namespace CassandraClient.Clusters
{
    public interface ICassandraCluster
    {
        IClusterConnection RetrieveClusterConnection();
        IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName);
        IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
    }
}