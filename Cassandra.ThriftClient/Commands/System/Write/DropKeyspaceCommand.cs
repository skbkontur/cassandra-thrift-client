using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.System.Write
{
    internal class DropKeyspaceCommand : CommandBase, IFierceCommand
    {
        public DropKeyspaceCommand(string keyspace)
        {
            this.keyspace = keyspace;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = cassandraClient.system_drop_keyspace(keyspace);
        }

        public string Output { get; private set; }
        private readonly string keyspace;
    }
}