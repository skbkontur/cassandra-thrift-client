using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

using CassandraClient.Clusters;
using CassandraClient.Exceptions;

namespace CassandraClient.Core.Pools
{
    public class ClusterConnectionPool : IClusterConnectionPool
    {
        public ClusterConnectionPool(ICassandraClusterSettings settings, Func<ICassandraClusterSettings, ConnectionPoolKey, IKeyspaceConnectionPool> createKeyspaceConnectionPool)
        {
            createPool = key => createKeyspaceConnectionPool(settings, key);
        }

        public IThriftConnection BorrowConnection(ConnectionPoolKey key)
        {

            var connectionPool = keyspacePools.GetOrAdd(key, createPool);
            IPooledThriftConnection result;
            if(!connectionPool.TryBorrowConnection(out result))
                throw new CassandraClientIOException(string.Format("Can't connect to endpoint '{0}' [keyspace={1}]", key.IpEndPoint, key.Keyspace));
            return result;
        }

        public IDictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            var result = new Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge>();
            foreach(var keyspaceConnectionPool in keyspacePools)
            {
                result.Add(keyspaceConnectionPool.Key, keyspaceConnectionPool.Value.GetKnowledge());
            }
            return result;
        }

        private readonly ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool> keyspacePools = new ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool>();
        private readonly Func<ConnectionPoolKey, IKeyspaceConnectionPool> createPool;
    }
}