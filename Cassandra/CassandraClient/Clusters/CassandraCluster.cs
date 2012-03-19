using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Clusters
{
    public class CassandraCluster : ICassandraCluster
    {
        public CassandraCluster(ICommandExecuter commandExecuter, ICassandraClusterSettings clusterSettings, ICassandraLogManager logManager)
        {
            this.commandExecuter = commandExecuter;
            this.clusterSettings = clusterSettings;
            this.logManager = logManager;
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return new ClusterConnection(commandExecuter,
                                         clusterSettings.ReadConsistencyLevel,
                                         clusterSettings.WriteConsistencyLevel,
                                         logManager);
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            return new KeyspaceConnection(commandExecuter,
                                          clusterSettings.ReadConsistencyLevel,
                                          clusterSettings.WriteConsistencyLevel,
                                          keyspaceName,
                                          logManager);
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            var columnFamilyConnectionImplementation = new ColumnFamilyConnectionImplementation(keySpaceName,
                                                                                                columnFamilyName,
                                                                                                clusterSettings,
                                                                                                commandExecuter,
                                                                                                clusterSettings.ReadConsistencyLevel,
                                                                                                clusterSettings.WriteConsistencyLevel);
            var enumerableFactory = new EnumerableFactory();
            return new ColumnFamilyConnection(columnFamilyConnectionImplementation, enumerableFactory);
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return commandExecuter.GetKnowledges();
        }

        public void CheckConnections()
        {
            commandExecuter.CheckConnections();
        }

        public void Dispose()
        {
            commandExecuter.Dispose();
        }

        private readonly ICassandraClusterSettings clusterSettings;
        private readonly ICassandraLogManager logManager;
        private readonly ICommandExecuter commandExecuter;
    }
}