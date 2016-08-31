using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal interface ICommandExecutor<in TCommand> : IDisposable
        where TCommand : ICommand
    {
        void Execute([NotNull] TCommand command);
        void Execute([NotNull] Func<int, TCommand> createCommand);
    }
}