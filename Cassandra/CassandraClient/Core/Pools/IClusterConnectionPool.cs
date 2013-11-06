using System;
using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    internal interface IClusterConnectionPool : IDisposable
    {
        IPooledThriftConnection BorrowConnection(ConnectionPoolKey key);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
    }
}