using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    public interface IKeyspaceConnectionPool : IDisposable
    {
        ConnectionType TryBorrowConnection(out IPooledThriftConnection thriftConnection);
        void ReleaseConnection(IPooledThriftConnection connection);
        KeyspaceConnectionPoolKnowledge GetKnowledge();
        void CheckConnections();
    }
}