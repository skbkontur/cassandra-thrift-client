using System;
using System.Collections.Concurrent;
using System.Net;

using CassandraClient.Clusters;
using CassandraClient.Exceptions;

using log4net;

namespace CassandraClient.Core.Pools
{
    public class KeyspaceConnectionPool : IKeyspaceConnectionPool
    {
        private readonly ILog logger = LogManager.GetLogger("KeyspaceConnectionPool");

        public KeyspaceConnectionPool(ICassandraClusterSettings settings, IPEndPoint endPoint, string keyspaceName)
        {
            this.endPoint = endPoint;
            this.keyspaceName = keyspaceName;
            this.settings = settings;
            logger.DebugFormat("Pool for node with endpoint {0} for keyspace '{1}' was created.", endPoint, keyspaceName);
        }

        public bool TryBorrowConnection(out PooledThriftConnection thriftConnection)
        {
            PooledThriftConnection result;
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

        public void ReleaseConnection(PooledThriftConnection connection)
        {
            PooledThriftConnection res;
            if (!busyConnections.TryRemove(connection.Id, out res))
                throw new FailedReleaseException(connection);
            freeConnections.Enqueue(connection);
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
        private readonly ConcurrentQueue<PooledThriftConnection> freeConnections = new ConcurrentQueue<PooledThriftConnection>();
        private readonly ConcurrentDictionary<Guid, PooledThriftConnection> busyConnections = new ConcurrentDictionary<Guid, PooledThriftConnection>();
    }
}