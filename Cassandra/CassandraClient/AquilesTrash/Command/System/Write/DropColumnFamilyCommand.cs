using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class DropColumnFamilyCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public DropColumnFamilyCommand(string keyspace, string columnFamily)
            : base(keyspace, columnFamily)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_drop_column_family(columnFamily);
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}