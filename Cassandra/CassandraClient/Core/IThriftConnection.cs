using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface IThriftConnection : IDisposable
    {
        void ExecuteCommand(ICommand command);
        bool IsAlive();
    }
}