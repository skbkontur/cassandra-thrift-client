using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetCountCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand
    {
        public GetCountCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, SlicePredicate predicate = null)
            : base(keyspace, columnFamily)
        {
            PartitionKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate ?? new SlicePredicate(new SliceRange {Count = int.MaxValue});
        }

        [NotNull]
        public byte[] PartitionKey { get; }

        public int QueriedPartitionsCount => 1;

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Count = cassandraClient.get_count(PartitionKey, BuildColumnParent(), predicate.ToCassandraSlicePredicate(), consistencyLevel);
        }

        public int Count { get; private set; }
        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
    }
}