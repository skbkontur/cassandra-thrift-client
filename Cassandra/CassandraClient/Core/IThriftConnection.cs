using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public interface IThriftConnection : IDisposable, ILiveness
    {
        void ExecuteCommand(ICommand command);

        DateTime CreationDateTime { get; }
    }
}