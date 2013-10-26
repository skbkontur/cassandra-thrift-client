using System.Collections.Generic;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.CassandraClient.Clusters
{
    public class CassandraCluster : ICassandraCluster
    {
        public CassandraCluster(ICassandraClusterSettings settings)
        {
            commandExecuter = new CommandExecuter(CreateDataConnectionPool(settings), CreateFierceConnectionPool(settings), settings);
            clusterSettings = settings;
        }

        private static IPoolSet<IThriftConnection, string> CreateDataConnectionPool(ICassandraClusterSettings settings)
        {
            var replicaSetPool = ReplicaSetPool.Create<IThriftConnection, string, IPEndPoint>(
                (key, replicaKey) => new Pool<IThriftConnection>(pool => new ThriftConnectionInPoolWrapper(settings.Timeout, replicaKey, key)),
                c => ((ThriftConnectionInPoolWrapper)c).ReplicaKey,
                c => ((ThriftConnectionInPoolWrapper)c).KeyspaceName
                );
            foreach(var endpoint in settings.Endpoints)
                replicaSetPool.RegisterReplica(endpoint);
            return replicaSetPool;
         }

        private static IPoolSet<IThriftConnection, string> CreateFierceConnectionPool(ICassandraClusterSettings settings)
        {
            var result = ReplicaSetPool.Create<IThriftConnection, string, IPEndPoint>(
                (key, replicaKey) => new Pool<IThriftConnection>(pool => new ThriftConnectionInPoolWrapper(settings.FierceTimeout, replicaKey, key)),
                c => ((ThriftConnectionInPoolWrapper)c).ReplicaKey,
                c => ((ThriftConnectionInPoolWrapper)c).KeyspaceName
                );
            result.RegisterReplica(settings.EndpointForFierceCommands);                
            return result;
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

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces)
        {
            new SchemeActualizer(this).ActualizeKeyspaces(keyspaces);
        }

        public void Dispose()
        {
            commandExecuter.Dispose();
        }

        private readonly ICassandraClusterSettings clusterSettings;
        private readonly ICommandExecuter commandExecuter;
    }
}