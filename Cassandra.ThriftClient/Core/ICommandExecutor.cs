using System;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Core
{
    internal interface ICommandExecutor<in TCommand> : IDisposable
        where TCommand : ICommand
    {
        void Execute([NotNull] TCommand command);
        void Execute([NotNull] Func<int, TCommand> createCommand);
    }
}