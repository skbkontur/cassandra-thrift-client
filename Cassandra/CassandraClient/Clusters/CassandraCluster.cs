using Aquiles;

using CassandraClient.Connections;

namespace CassandraClient.Clusters
{
    public class CassandraCluster : ICassandraCluster
    {
        public CassandraCluster(ICassandraClusterSettings clusterSettings)
        {
            clusterName = clusterSettings.Name;
            this.clusterSettings = clusterSettings;
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return new ClusterConnection(AquilesHelper.RetrieveConnection(clusterName),
                                         clusterSettings.ClusterReadConsistencyLevel, clusterSettings.ClusterWriteConsistencyLevel);
        }

        public IClusterConnection RetrieveKeyspaceConnection(string keyspace)
        {
            return new ClusterConnection(AquilesHelper.RetrieveConnection(clusterName, keyspace),
                                         clusterSettings.ClusterReadConsistencyLevel, clusterSettings.ClusterWriteConsistencyLevel);
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            var columnFamilyConnectionImplementation = new ColumnFamilyConnectionImplementation(columnFamilyName, AquilesHelper.RetrieveConnection(clusterName, keySpaceName),
                                                                                                clusterSettings.ColumnFamilyReadConsistencyLevel, clusterSettings.ColumnFamilyWriteConsistencyLevel);
            return new ColumnFamilyConnection(columnFamilyConnectionImplementation);
        }

        private readonly string clusterName;
        private readonly ICassandraClusterSettings clusterSettings;
    }
}