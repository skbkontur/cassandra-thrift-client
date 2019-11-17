using SkbKontur.Cassandra.ThriftClient.Abstractions;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.Base
{
    internal abstract class CommandBase : ICommand
    {
        public abstract void Execute(Apache.Cassandra.Cassandra.Client client, ILog logger);
        public string Name => GetType().Name;
        public virtual CommandContext CommandContext => new CommandContext();
    }
}