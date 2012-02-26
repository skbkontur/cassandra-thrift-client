using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface IThriftConnection : IDisposable
    {
        void ExecuteCommand(ICommand command, ICassandraLogger logger);
        bool IsAlive { get; }
        void Check();
    }
}