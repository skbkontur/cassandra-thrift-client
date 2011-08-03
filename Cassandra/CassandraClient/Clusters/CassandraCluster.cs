using CassandraClient.Connections;
using CassandraClient.Core;

namespace CassandraClient.Clusters
{
    public class CassandraCluster : ICassandraCluster
    {
        public CassandraCluster(ICommandExecuter commandExecuter, ICassandraClusterSettings clusterSettings)
        {
            this.commandExecuter = commandExecuter;
            this.clusterSettings = clusterSettings;
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return new ClusterConnection(commandExecuter,
                                         clusterSettings.ReadConsistencyLevel, clusterSettings.WriteConsistencyLevel);
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            return new KeyspaceConnection(commandExecuter,
                                          clusterSettings.ReadConsistencyLevel,
                                          clusterSettings.WriteConsistencyLevel,
                                          keyspaceName);
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            var columnFamilyConnectionImplementation = new ColumnFamilyConnectionImplementation(keySpaceName, columnFamilyName, commandExecuter,
                                                                                                clusterSettings.ReadConsistencyLevel, clusterSettings.WriteConsistencyLevel);
            return new ColumnFamilyConnection(columnFamilyConnectionImplementation);
        }

        private readonly ICassandraClusterSettings clusterSettings;
        private readonly ICommandExecuter commandExecuter;
    }
}