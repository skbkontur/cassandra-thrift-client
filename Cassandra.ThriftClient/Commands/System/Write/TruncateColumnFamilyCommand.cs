using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.System.Write
{
    internal class TruncateColumnFamilyCommand : KeyspaceColumnFamilyDependantCommandBase, IFierceCommand
    {
        public TruncateColumnFamilyCommand(string keyspace, string columnFamily)
            : base(keyspace, columnFamily)
        {
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            cassandraClient.truncate(columnFamily);
        }
    }
}