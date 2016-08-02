using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class UpdateKeyspaceCommand : CommandBase, IFierceCommand
    {
        public UpdateKeyspaceCommand(Keyspace keyspaceDefinition)
        {
            this.keyspaceDefinition = keyspaceDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_update_keyspace(keyspaceDefinition.ToCassandraKsDef());
        }

        public string Output { get; private set; }
        private readonly Keyspace keyspaceDefinition;
    }
}