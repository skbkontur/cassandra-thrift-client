using System.Collections.Generic;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public interface IClusterConnectionPool
    {
        IThriftConnection BorrowConnection(ConnectionPoolKey key);
        Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
    }
}