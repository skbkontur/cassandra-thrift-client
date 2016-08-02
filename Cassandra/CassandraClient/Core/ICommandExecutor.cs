using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal interface ICommandExecutor<in TCommand> : IDisposable
        where TCommand : ICommand
    {
        void Execute(Func<int, TCommand> createCommand);
        void Execute(TCommand command);
    }
}