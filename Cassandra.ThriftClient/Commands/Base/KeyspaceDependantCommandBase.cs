using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.Base
{
    internal abstract class KeyspaceDependantCommandBase : CommandBase
    {
        protected KeyspaceDependantCommandBase(string keyspace)
        {
            this.keyspace = keyspace;
        }

        public override CommandContext CommandContext => new CommandContext
            {
                KeyspaceName = keyspace
            };

        protected readonly string keyspace;
    }
}