using System.Net;

namespace CassandraClient.Core.Pools
{
    public interface IClusterConnectionPool
    {
        PooledThriftConnection BorrowConnection(IPEndPoint endPoint, string keyspace);
    }
}