using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal interface ICommandExecuter : IDisposable
    {
        void Execute(ICommand command);
        void Execute(Func<int, ICommand> createCommand);
        void ExecuteSchemeUpdateCommandOnce(ISchemeUpdateCommand command);
    }
}