using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public class ClusterConnectionPool : IClusterConnectionPool
    {
        public ClusterConnectionPool(ICassandraLogManager logManager, ICassandraClusterSettings settings, Func<ICassandraClusterSettings, ConnectionPoolKey, IKeyspaceConnectionPool> createKeyspaceConnectionPool)
        {
            createPool = key => createKeyspaceConnectionPool(settings, key);
            logger = logManager.GetLogger(GetType());
        }

        public IThriftConnection BorrowConnection(ConnectionPoolKey key)
        {
            var connectionPool = keyspacePools.GetOrAdd(key, createPool);
            IPooledThriftConnection result;
            var connectionType = connectionPool.TryBorrowConnection(out result);
            if(connectionType == ConnectionType.Undefined)
                throw new CassandraClientIOException(string.Format("Can't connect to endpoint '{0}' [keyspace={1}]", key.IpEndPoint, key.Keyspace));
            if (connectionType == ConnectionType.New)
            {
                logger.Debug("Added new connection {0}.{1}{2}", result, Environment.NewLine, KnowledgesToString(GetKnowledges()));
            }
            return result;
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            var result = new Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge>();
            foreach(var keyspaceConnectionPool in keyspacePools)
            {
                result.Add(keyspaceConnectionPool.Key, keyspaceConnectionPool.Value.GetKnowledge());
            }
            return result;
        }

        public void CheckConnections()
        {
            foreach (var keyspaceConnectionPool in keyspacePools.Values)
            {
                keyspaceConnectionPool.CheckConnections();
            }
        }

        private string KnowledgesToString(Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> knowledges)
        {
            var result = "";
            foreach(var kvp in knowledges)
            {
                if (result.Length > 0)
                    result += Environment.NewLine;
                result += kvp.Key + "; " + kvp.Value;
            }
            return result;
        }

        private readonly ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool> keyspacePools = new ConcurrentDictionary<ConnectionPoolKey, IKeyspaceConnectionPool>();
        private readonly Func<ConnectionPoolKey, IKeyspaceConnectionPool> createPool;
        private readonly ICassandraLogger logger;
    }
}