﻿using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;

namespace SKBKontur.Cassandra.CassandraClient.Clusters
{
    public class CassandraCluster : ICassandraCluster
    {
        public CassandraCluster(ICassandraClusterSettings settings)
            : this(new CommandExecuter(new ClusterConnectionPool(k => new KeyspaceConnectionPool(settings, k)), new EndpointManager(new Badlist()), settings), settings)
        {
        }

        private CassandraCluster(ICommandExecuter commandExecuter, ICassandraClusterSettings clusterSettings)
        {
            this.commandExecuter = commandExecuter;
            this.clusterSettings = clusterSettings;
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return new ClusterConnection(commandExecuter);
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            return new KeyspaceConnection(commandExecuter, keyspaceName);
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
        private readonly ICommandExecuter commandExecuter;
    }
}