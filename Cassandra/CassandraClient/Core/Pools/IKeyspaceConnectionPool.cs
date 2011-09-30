namespace CassandraClient.Core.Pools
{
    public interface IKeyspaceConnectionPool
    {
        bool TryBorrowConnection(out PooledThriftConnection thriftConnection);
        void ReleaseConnection(PooledThriftConnection connection);
    }
}