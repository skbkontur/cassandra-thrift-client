using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;
using SliceRange = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SliceRange;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    internal class GetCountCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetCountCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, SlicePredicate predicate = null)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate ?? new SlicePredicate(new SliceRange {Count = int.MaxValue});
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Count = cassandraClient.get_count(rowKey, BuildColumnParent(), predicate.ToCassandraSlicePredicate(), consistencyLevel);
        }

        public int Count { get; private set; }
        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
    }
}