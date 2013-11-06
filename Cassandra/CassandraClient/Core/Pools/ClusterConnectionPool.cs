using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Exceptions;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    internal class ClusterConnectionPool : IClusterConnectionPool
    {
        public ClusterConnectionPool(Func<ConnectionPoolKey, IKeyspaceConnectionPool> createKeyspaceConnectionPool)
        {
            createPool = createKeyspaceConnectionPool;
            logger = LogManager.GetLogger(typeof(ClusterConnectionPool));
        }

        public IPooledThriftConnection BorrowConnection(ConnectionPoolKey key)
        {
            var keyspaceConnectionPool = keyspacePools.GetOrAdd(key, createPool);
            IPooledThriftConnection result;
            var connectionType = keyspaceConnectionPool.TryBorrowConnection(out result);
            if(connectionType == ConnectionType.Undefined)
                throw new CassandraClientIOException(string.Format("Can't connect to endpoint '{0}' [keyspace={1}]", key.IpEndPoint, key.Keyspace));
            if(connectionType == ConnectionType.New)
                logger.DebugFormat("Added new connection {0}.{1}{2}", result, Environment.NewLine, KnowledgesToString(GetKnowledges()));
            return result;
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            var result = new Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge>();
            foreach(var keyspaceConnectionPool in keyspacePools)
                result.Add(keyspaceConnectionPool.Key, keyspaceConnectionPool.Value.GetKnowledge());
            return result;
        }

        public void Dispose()
        {
            foreach(var keyspaceConnectionPool in keyspacePools.Values)
                keyspaceConnectionPool.Dispose();
        }

        private string KnowledgesToString(Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> knowledges)
        {
            var result = "";
            foreach(var kvp in knowledges)
            {
                if(result.Length > 0)
                    result += Environment.NewLine;
                result += kvp.Key + "; " + kvp.Value;
            }
            return result;
        }

        private readonly ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool> keyspacePools = new ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool>();
        private readonly Func<ConnectionPoolKey, IKeyspaceConnectionPool> createPool;
        private readonly ILog logger;
    }
}