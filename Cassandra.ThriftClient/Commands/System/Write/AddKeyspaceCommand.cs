using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class AddKeyspaceCommand : CommandBase, IFierceCommand
    {
        public AddKeyspaceCommand(Keyspace keyspaceDefinition)
        {
            this.keyspaceDefinition = keyspaceDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = cassandraClient.system_add_keyspace(keyspaceDefinition.ToCassandraKsDef());
        }

        public string Output { get; private set; }
        private readonly Keyspace keyspaceDefinition;
    }
}