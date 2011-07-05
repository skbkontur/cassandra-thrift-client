using CassandraClient.Connections;

namespace CassandraClient.Clusters
{
    public interface ICassandraCluster
    {
        IClusterConnection RetrieveClusterConnection();
        IClusterConnection RetrieveKeyspaceConnection(string keyspace);
        IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName);
    }
}