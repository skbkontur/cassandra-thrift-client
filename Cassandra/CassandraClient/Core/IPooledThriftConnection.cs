using System;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface IPooledThriftConnection : IThriftConnection
    {
        Guid Id { get; }
    }
}