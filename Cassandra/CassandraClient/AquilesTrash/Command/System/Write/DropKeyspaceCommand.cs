using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class DropKeyspaceCommand : CommandBase
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
        public override bool IsFierce { get { return true; } }
        private readonly string keyspace;
    }
}