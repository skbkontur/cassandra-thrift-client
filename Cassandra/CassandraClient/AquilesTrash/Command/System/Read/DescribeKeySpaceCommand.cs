using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Read
{
    public class DescribeKeyspaceCommand : KeyspaceDependantCommandBase
    {
        public DescribeKeyspaceCommand(string keyspace)
            : base(keyspace)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var keyspaceDescription = cassandraClient.describe_keyspace(keyspace);
            KeyspaceInformation = keyspaceDescription.FromCassandraKsDef();
        }

        public override bool IsFierce { get { return true; } }
        public Keyspace KeyspaceInformation { get; private set; }
    }
}