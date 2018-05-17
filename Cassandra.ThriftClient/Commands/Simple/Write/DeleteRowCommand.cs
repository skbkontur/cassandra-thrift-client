using Apache.Cassandra;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;
using SKBKontur.Cassandra.CassandraClient.Core;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write
{
    internal class DeleteRowCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand
    {
        public DeleteRowCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, long? timestamp)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.timestamp = timestamp;
        }

        [NotNull]
        public byte[] PartitionKey { get { return rowKey; } }

        public int QueriedPartitionsCount { get { return 1; } }

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