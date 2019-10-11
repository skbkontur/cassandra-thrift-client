using SKBKontur.Cassandra.CassandraClient.Abstractions;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Base
{
    internal abstract class CommandBase : ICommand
    {
        public abstract void Execute(Apache.Cassandra.Cassandra.Client client, ILog logger);
        public string Name => GetType().Name;
        public virtual CommandContext CommandContext => new CommandContext();
    }
}