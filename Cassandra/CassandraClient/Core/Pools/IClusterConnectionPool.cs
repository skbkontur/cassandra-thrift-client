using System.Net;

namespace CassandraClient.Core.Pools
{
    public interface IClusterConnectionPool
    {
        IThriftConnection BorrowConnection(IPEndPoint endPoint, string keyspace);
    }
}