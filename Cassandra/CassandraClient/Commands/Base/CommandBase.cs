using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Base
{
    internal abstract class CommandBase : ICommand
    {
        public abstract void Execute(Apache.Cassandra.Cassandra.Client client);
        public string Name { get { return GetType().Name; } }
        public virtual CommandContext CommandContext { get { return new CommandContext(); } }
    }
}