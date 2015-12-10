using System.Collections.Generic;
using System.Linq;
using System.Net;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener;
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
            dataCommandsConnectionPool = CreateDataConnectionPool(settings);
            fierceCommandsConnectionPool = CreateFierceConnectionPool(settings);
            commandExecuter = new CommandExecuter(dataCommandsConnectionPool, fierceCommandsConnectionPool, settings);
            clusterSettings = settings;
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
            return new ColumnFamilyConnection(columnFamilyConnectionImplementation);
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            var dataConnectionKnowledges = dataCommandsConnectionPool.GetActiveItemsInfo();
            var fierceConnectionKnowledges = fierceCommandsConnectionPool.GetActiveItemsInfo();

            var result = dataConnectionKnowledges.ToDictionary(
                knowledgePair => new ConnectionPoolKey {IsFierce = false, IpEndPoint = knowledgePair.Key.ReplicaKey, Keyspace = knowledgePair.Key.ItemKey},
                knowledgePair => knowledgePair.Value);

            foreach(var knowledgePair in fierceConnectionKnowledges)
                result.Add(new ConnectionPoolKey {IsFierce = true, IpEndPoint = knowledgePair.Key.ReplicaKey, Keyspace = knowledgePair.Key.ItemKey}, knowledgePair.Value);

            return result;
        }

        public void ActualizeKeyspaces(KeyspaceScheme[] keyspaces, ICassandraActualizerEventListener eventListener = null)
        {
            new SchemeActualizer(this, eventListener).ActualizeKeyspaces(keyspaces);
        }

        public void Dispose()
        {
            commandExecuter.Dispose();
        }

        public IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName)
        {
            return new ColumnFamilyConnectionImplementation(keySpaceName,
                                                            columnFamilyName,
                                                            clusterSettings,
                                                            commandExecuter,
                                                            clusterSettings.ReadConsistencyLevel,
                                                            clusterSettings.WriteConsistencyLevel);
        }

        private ReplicaSetPool<IThriftConnection, string, IPEndPoint> CreateDataConnectionPool(ICassandraClusterSettings settings)
        {
            var replicaSetPool = ReplicaSetPool.Create<IThriftConnection, string, IPEndPoint>(
                settings.Endpoints,
                (key, replicaKey) => GetDataConnectionPool(settings, replicaKey, key),
                c => ((ThriftConnectionInPoolWrapper)c).ReplicaKey,
                c => ((ThriftConnectionInPoolWrapper)c).KeyspaceName,
                settings.ConnectionIdleTimeout
                );
            return replicaSetPool;
        }

        private ReplicaSetPool<IThriftConnection, string, IPEndPoint> CreateFierceConnectionPool(ICassandraClusterSettings settings)
        {
            var result = ReplicaSetPool.Create<IThriftConnection, string, IPEndPoint>(
                new[] {settings.EndpointForFierceCommands},
                (key, replicaKey) => CreateFiercePool(settings, replicaKey, key),
                c => ((ThriftConnectionInPoolWrapper)c).ReplicaKey,
                c => ((ThriftConnectionInPoolWrapper)c).KeyspaceName,
                settings.ConnectionIdleTimeout
                );
            return result;
        }

        private Pool<IThriftConnection> GetDataConnectionPool(ICassandraClusterSettings settings, IPEndPoint nodeEndpoint, string keyspaceName)
        {
            var result = new Pool<IThriftConnection>(pool => CreateThriftConnection(nodeEndpoint, keyspaceName, settings.Timeout));
            logger.DebugFormat("Pool for node with endpoint {0} for keyspace '{1}' was created.", nodeEndpoint, keyspaceName);
            return result;
        }

        private ThriftConnectionInPoolWrapper CreateThriftConnection(IPEndPoint nodeEndpoint, string keyspaceName, int timeout)
        {
            var result = new ThriftConnectionInPoolWrapper(timeout, nodeEndpoint, keyspaceName);
            logger.DebugFormat("Connection {0} was created.", result);
            return result;
        }

        private Pool<IThriftConnection> CreateFiercePool(ICassandraClusterSettings settings, IPEndPoint nodeEndpoint, string keyspaceName)
        {
            var result = new Pool<IThriftConnection>(pool => CreateThriftConnection(nodeEndpoint, keyspaceName, settings.FierceTimeout));
            logger.DebugFormat("Pool for node with endpoint {0} for keyspace '{1}'[Fierce] was created.", nodeEndpoint, keyspaceName);
            return result;
        }

        private readonly ICassandraClusterSettings clusterSettings;
        private readonly ICommandExecuter commandExecuter;
        private readonly ReplicaSetPool<IThriftConnection, string, IPEndPoint> dataCommandsConnectionPool;
        private readonly ReplicaSetPool<IThriftConnection, string, IPEndPoint> fierceCommandsConnectionPool;
        private readonly ILog logger = LogManager.GetLogger(typeof(CassandraCluster));
    }
}