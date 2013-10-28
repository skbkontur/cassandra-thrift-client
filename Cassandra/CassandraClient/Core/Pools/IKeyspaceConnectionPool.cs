using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.Pools
{
    internal interface IKeyspaceConnectionPool : IDisposable
    {
        ConnectionType TryBorrowConnection(out IPooledThriftConnection thriftConnection);
        void ReleaseConnection(IPooledThriftConnection connection);
        KeyspaceConnectionPoolKnowledge GetKnowledge();
        void CheckConnections();
    }
}