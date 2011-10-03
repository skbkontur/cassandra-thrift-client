namespace CassandraClient.Core.Pools
{
    public interface IKeyspaceConnectionPool
    {
        bool TryBorrowConnection(out IPooledThriftConnection thriftConnection);
        void ReleaseConnection(IPooledThriftConnection connection);
        KeyspaceConnectionPoolKnowledge GetKnowledge();
    }
}