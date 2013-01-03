using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Write
{
    public class DeleteRowCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public DeleteRowCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, long? timestamp)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.timestamp = timestamp;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.remove(
                rowKey,
                new ColumnPath {Column_family = columnFamily},
                timestamp ?? DateTimeService.UtcNow.Ticks,
                consistencyLevel);
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly long? timestamp;
    }
}