using System;
using System.Collections.Concurrent;
using System.Net;

using CassandraClient.Clusters;
using CassandraClient.Exceptions;

namespace CassandraClient.Core.Pools
{
    public class ClusterConnectionPool : IClusterConnectionPool
    {
        public ClusterConnectionPool(ICassandraClusterSettings settings)
        {
            createPool = key1 => new KeyspaceConnectionPool(settings, key1.IpEndPoint, key1.Keyspace);
        }

        public IThriftConnection BorrowConnection(IPEndPoint endPoint, string keyspace)
        {
            var key = new ConnectionPoolKey
                {
                    IpEndPoint = endPoint,
                    Keyspace = keyspace
                };

            IKeyspaceConnectionPool connectionPool = keyspacePools.GetOrAdd(key, createPool);
            PooledThriftConnection result;
            if(!connectionPool.TryBorrowConnection(out result))
                throw new CassandraClientIOException(string.Format("Can't connect to endpoint '{0}' [keyspace={1}]", endPoint, keyspace));
            return result;
        }

        private readonly ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool> keyspacePools = new ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool>();
        private readonly Func<ConnectionPoolKey, IKeyspaceConnectionPool> createPool;
    }
}