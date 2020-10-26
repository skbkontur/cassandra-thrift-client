using System.Collections.Generic;
using System.Linq;
using System.Net;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Connections;
using SkbKontur.Cassandra.ThriftClient.Core;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool;
using SkbKontur.Cassandra.ThriftClient.Core.Metrics;
using SkbKontur.Cassandra.ThriftClient.Core.Pools;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Clusters
{
    [PublicAPI]
    public class CassandraCluster : ICassandraCluster
    {
        public CassandraCluster(ICassandraClusterSettings settings, ILog logger)
        {
            this.logger = logger.ForContext("CassandraThriftClient");
            dataCommandsConnectionPool = CreateDataConnectionPool(settings);
            fierceCommandsConnectionPool = CreateFierceConnectionPool(settings);
            commandExecutor = new SimpleCommandExecutor(dataCommandsConnectionPool, settings, logger);
            fierceCommandExecutor = new FierceCommandExecutor(fierceCommandsConnectionPool, settings);
            clusterSettings = settings;
        }

        public IClusterConnection RetrieveClusterConnection()
        {
            return new ClusterConnection(fierceCommandExecutor, logger);
        }

        public IKeyspaceConnection RetrieveKeyspaceConnection(string keyspaceName)
        {
            return new KeyspaceConnection(fierceCommandExecutor, keyspaceName);
        }

        public IColumnFamilyConnection RetrieveColumnFamilyConnection(string keySpaceName, string columnFamilyName)
        {
            var columnFamilyConnectionImplementation = RetrieveColumnFamilyConnectionImplementation(keySpaceName, columnFamilyName);
            return new ColumnFamilyConnection(columnFamilyConnectionImplementation);
        }

        public ITimeBasedColumnFamilyConnection RetrieveTimeBasedColumnFamilyConnection(string keyspace, string columnFamily)
        {
            var columnFamilyConnectionImplementation = RetrieveColumnFamilyConnectionImplementation(keyspace, columnFamily);
            return new TimeBasedColumnFamilyConnection(columnFamilyConnectionImplementation);
        }

        public IColumnFamilyConnectionImplementation RetrieveColumnFamilyConnectionImplementation(string keySpaceName, string columnFamilyName)
        {
            return new ColumnFamilyConnectionImplementation(keySpaceName, columnFamilyName, clusterSettings, commandExecutor, fierceCommandExecutor);
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            var dataConnectionKnowledges = dataCommandsConnectionPool.GetActiveItemsInfo();
            var fierceConnectionKnowledges = fierceCommandsConnectionPool.GetActiveItemsInfo();

            var result = dataConnectionKnowledges.ToDictionary(
                knowledgePair => new ConnectionPoolKey {IsFierce = false, IpEndPoint = knowledgePair.Key.ReplicaKey, Keyspace = knowledgePair.Key.ItemKey},
                knowledgePair => knowledgePair.Value);

            foreach (var knowledgePair in fierceConnectionKnowledges)
                result.Add(new ConnectionPoolKey {IsFierce = true, IpEndPoint = knowledgePair.Key.ReplicaKey, Keyspace = knowledgePair.Key.ItemKey}, knowledgePair.Value);

            return result;
        }

        public void Dispose()
        {
            commandExecutor.Dispose();
            fierceCommandExecutor.Dispose();
        }

        private ReplicaSetPool<IThriftConnection, string, IPEndPoint> CreateDataConnectionPool(ICassandraClusterSettings settings)
        {
            var replicaSetPool = ReplicaSetPool.Create<IThriftConnection, string, IPEndPoint>(
                settings.Endpoints,
                (key, replicaKey) => GetDataConnectionPool(settings, replicaKey, key),
                c => ((ThriftConnectionInPoolWrapper)c).ReplicaKey,
                c => ((ThriftConnectionInPoolWrapper)c).KeyspaceName,
                settings.ConnectionIdleTimeout,
                PoolSettings.CreateDefault(),
                logger);
            return replicaSetPool;
        }

        private ReplicaSetPool<IThriftConnection, string, IPEndPoint> CreateFierceConnectionPool(ICassandraClusterSettings settings)
        {
            var result = ReplicaSetPool.Create<IThriftConnection, string, IPEndPoint>(
                new[] {settings.EndpointForFierceCommands},
                (key, replicaKey) => CreateFiercePool(settings, replicaKey, key),
                c => ((ThriftConnectionInPoolWrapper)c).ReplicaKey,
                c => ((ThriftConnectionInPoolWrapper)c).KeyspaceName,
                settings.ConnectionIdleTimeout,
                PoolSettings.CreateDefault(),
                logger);
            return result;
        }

        private Pool<IThriftConnection> GetDataConnectionPool(ICassandraClusterSettings settings, IPEndPoint nodeEndpoint, string keyspaceName)
        {
            var result = CreateConnectionPool(settings, nodeEndpoint, keyspaceName, settings.Timeout);
            logger.Debug("Pool for node with endpoint {0} for keyspace '{1}' was created.", nodeEndpoint, keyspaceName);
            return result;
        }

        private ThriftConnectionInPoolWrapper CreateThriftConnection(IPEndPoint nodeEndpoint, string keyspaceName, int timeout, Credentials credentials)
        {
            var result = new ThriftConnectionInPoolWrapper(timeout, nodeEndpoint, keyspaceName, credentials, logger);
            logger.Debug("Connection {0} was created.", result);
            return result;
        }

        private Pool<IThriftConnection> CreateFiercePool(ICassandraClusterSettings settings, IPEndPoint nodeEndpoint, string keyspaceName)
        {
            var result = CreateConnectionPool(settings, nodeEndpoint, keyspaceName, settings.FierceTimeout);
            logger.Debug("Pool for node with endpoint {0} for keyspace '{1}'[Fierce] was created.", nodeEndpoint, keyspaceName);
            return result;
        }

        private Pool<IThriftConnection> CreateConnectionPool(ICassandraClusterSettings settings, IPEndPoint nodeEndpoint, string keyspaceName, int timeout)
        {
            var connectionPoolMetrics = CommandMetricsFactory.GetConnectionPoolMetrics(settings, nodeEndpoint.Address.ToString().Replace('.', '_'), keyspaceName);
            return new Pool<IThriftConnection>(
                pool => CreateThriftConnection(nodeEndpoint, keyspaceName, timeout, settings.Credentials),
                connectionPoolMetrics,
                logger);
        }

        private readonly ICassandraClusterSettings clusterSettings;
        private readonly ILog logger;
        private readonly ICommandExecutor<ISimpleCommand> commandExecutor;
        private readonly ICommandExecutor<IFierceCommand> fierceCommandExecutor;
        private readonly ReplicaSetPool<IThriftConnection, string, IPEndPoint> dataCommandsConnectionPool;
        private readonly ReplicaSetPool<IThriftConnection, string, IPEndPoint> fierceCommandsConnectionPool;
    }
}