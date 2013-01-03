using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class TruncateColumnFamilyCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public TruncateColumnFamilyCommand(string keyspace, string columnFamily)
            : base(keyspace, columnFamily)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.truncate(columnFamily);
        }

        public override bool IsFierce { get { return true; } }
    }
}
