using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface IPooledThriftConnection : IDisposable
    {
        Guid Id { get; }
        bool IsAlive { get; set; }
        void ExecuteCommand(ICommand command, ICassandraLogger logger);
        bool Ping();
        void Kill();
    }
}