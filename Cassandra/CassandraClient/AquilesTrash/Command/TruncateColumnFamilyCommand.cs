namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class TruncateColumnFamilyCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.truncate(ColumnFamily);
        }
    }
}
