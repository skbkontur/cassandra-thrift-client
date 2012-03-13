using System;
using System.Collections.Concurrent;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class KeyspaceConnectionPool : IKeyspaceConnectionPool
    {
        public KeyspaceConnectionPool(ICassandraClusterSettings settings, ConnectionPoolKey key, ICassandraLogManager logManager)
        {
            logger = logManager.GetLogger(GetType());
            endPoint = key.IpEndPoint;
            keyspaceName = key.Keyspace;
            isFierce = key.IsFierce;
            this.settings = settings;
            this.logManager = logManager;
            logger.Debug("Pool for node with endpoint {0} for keyspace '{1}'{2} was created.", endPoint, keyspaceName, isFierce?"[Fierce]":"");
        }

        public ConnectionType TryBorrowConnection(out IPooledThriftConnection thriftConnection)
        {
            IPooledThriftConnection result;
            ConnectionType connectionType;
            if (!freeConnections.TryDequeue(out result))
            {
                result = CreateConnection();
                if (!result.IsAlive)
                {
                    thriftConnection = null;
                    return ConnectionType.Undefined;
                }
                connectionType = ConnectionType.New;
            }
            else
            {
                if (!result.IsAlive)
                {
                    return TryBorrowConnection(out thriftConnection);
                }
                connectionType = ConnectionType.FromPool;
            }

            if (!busyConnections.TryAdd(result.Id, result))
                throw new GuidCollisionException(result.Id);
            thriftConnection = result;
            return connectionType;
        }

        public void ReleaseConnection(IPooledThriftConnection connection)
        {
            IPooledThriftConnection res;
            if (!busyConnections.TryRemove(connection.Id, out res))
                throw new FailedReleaseException(connection);
            freeConnections.Enqueue(connection);
        }

        public KeyspaceConnectionPoolKnowledge GetKnowledge()
        {
            return new KeyspaceConnectionPoolKnowledge
                {
                    BusyConnectionCount = busyConnections.Count,
                    FreeConnectionCount = freeConnections.Count
                };
        }

        public void CheckConnections()
        {
            var connections = freeConnections.ToArray();
            foreach (var connection in connections)
            {
                connection.IsAlive = connection.Ping();
            }
        }

        private PooledThriftConnection CreateConnection()
        {
            var pooledThriftConnection = new PooledThriftConnection(this, isFierce ? settings.FierceTimeout : settings.Timeout, endPoint, keyspaceName, logManager);
            logger.Debug("Connection {0} was created.", pooledThriftConnection);
            return pooledThriftConnection;
        }

        private readonly ICassandraClusterSettings settings;
        private readonly ICassandraLogManager logManager;
        private readonly IPEndPoint endPoint;
        private readonly string keyspaceName;
        private readonly bool isFierce;
        private readonly ConcurrentQueue<IPooledThriftConnection> freeConnections = new ConcurrentQueue<IPooledThriftConnection>();
        private readonly ConcurrentDictionary<Guid, IPooledThriftConnection> busyConnections = new ConcurrentDictionary<Guid, IPooledThriftConnection>();
        private readonly ICassandraLogger logger;
    }
}