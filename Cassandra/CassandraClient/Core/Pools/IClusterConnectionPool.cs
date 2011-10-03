using System.Collections.Generic;

namespace CassandraClient.Core.Pools
{
    public interface IClusterConnectionPool
    {
        IThriftConnection BorrowConnection(ConnectionPoolKey key);
        IDictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges();
    }
}