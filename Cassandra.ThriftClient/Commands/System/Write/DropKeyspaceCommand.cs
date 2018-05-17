using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class DropKeyspaceCommand : CommandBase, IFierceCommand
    {
        public DropKeyspaceCommand(string keyspace)
        {
            this.keyspace = keyspace;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_drop_keyspace(keyspace);
        }

        public string Output { get; private set; }
        private readonly string keyspace;
    }
}