using Apache.Cassandra;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using SkbKontur.Cassandra.TimeBasedUuid;

using Vostok.Logging.Abstractions;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write
{
    internal class DeleteRowCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand
    {
        public DeleteRowCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, long? timestamp)
            : base(keyspace, columnFamily)
        {
            PartitionKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.timestamp = timestamp;
        }

        [NotNull]
        public byte[] PartitionKey { get; }

        public int QueriedPartitionsCount => 1;
        public long? ResponseSize => null;

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            cassandraClient.remove(
                PartitionKey,
                new ColumnPath {Column_family = columnFamily},
                timestamp ?? Timestamp.Now.Ticks,
                consistencyLevel);
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly long? timestamp;
    }
}