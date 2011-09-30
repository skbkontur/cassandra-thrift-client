using CassandraClient.Connections;

namespace CassandraClient.Clusters
{
    public interface ICassandraCluster
    {
        IClusterConnection RetrieveClusterConnection();
        IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName);
        IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName);
    }
}