using System;

namespace CassandraClient.Core
{
    public interface IPooledThriftConnection : IThriftConnection
    {
        Guid Id { get; }
    }
}