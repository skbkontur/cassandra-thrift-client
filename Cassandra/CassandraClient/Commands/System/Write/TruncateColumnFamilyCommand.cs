using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class TruncateColumnFamilyCommand : KeyspaceColumnFamilyDependantCommandBase, IFierceCommand
    {
        public TruncateColumnFamilyCommand(string keyspace, string columnFamily)
            : base(keyspace, columnFamily)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.truncate(columnFamily);
        }
    }
}