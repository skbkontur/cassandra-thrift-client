using System;
using System.Collections.Concurrent;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class KeyspaceConnectionPool : IKeyspaceConnectionPool
    {
        private readonly ILog logger = LogManager.GetLogger("KeyspaceConnectionPool");

        public KeyspaceConnectionPool(ICassandraClusterSettings settings, ConnectionPoolKey key)
        {
            endPoint = key.IpEndPoint;
            keyspaceName = key.Keyspace;
            this.settings = settings;
            logger.DebugFormat("Pool for node with endpoint {0} for keyspace '{1}' was created.", endPoint, keyspaceName);
        }

        public bool TryBorrowConnection(out IPooledThriftConnection thriftConnection)
        {
            IPooledThriftConnection result;
            if (!freeConnections.TryDequeue(out result))
            {
                result = CreateConnection();
                if (!result.IsAlive())
                {
                    thriftConnection = null;
                    return false;
                }
            }
            else
            {
                if (!result.IsAlive())
                {
                    return TryBorrowConnection(out thriftConnection);
                }
            }

            if (!busyConnections.TryAdd(result.Id, result))
                throw new GuidCollisionException(result.Id);
            thriftConnection = result;
            return true;
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

        private PooledThriftConnection CreateConnection()
        {
            var pooledThriftConnection = new PooledThriftConnection(this, settings.Timeout, endPoint, keyspaceName);
            logger.DebugFormat("Connection {0} was created.", pooledThriftConnection);
            return pooledThriftConnection;
        }

        private readonly ICassandraClusterSettings settings;
        private readonly IPEndPoint endPoint;
        private readonly string keyspaceName;
        private readonly ConcurrentQueue<IPooledThriftConnection> freeConnections = new ConcurrentQueue<IPooledThriftConnection>();
        private readonly ConcurrentDictionary<Guid, IPooledThriftConnection> busyConnections = new ConcurrentDictionary<Guid, IPooledThriftConnection>();
    }
}