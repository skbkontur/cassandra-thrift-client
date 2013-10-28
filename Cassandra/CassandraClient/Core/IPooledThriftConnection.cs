using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal interface IPooledThriftConnection : IDisposable
    {
        Guid Id { get; }
        bool IsAlive { get; }
        void ExecuteCommand(ICommand command);
        void Kill();
    }
}