using System.Collections.Generic;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;
using SliceRange = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SliceRange;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class MultiGetCountCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public MultiGetCountCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate ?? new SlicePredicate(new SliceRange {Count = int.MaxValue});
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.multiget_count(keys, BuildColumnParent(), predicate.ToCassandraSlicePredicate(), consistencyLevel);
        }

        public Dictionary<byte[], int> Output { get; private set; }
        public override int QueriedPartitionsCount { get { return Output.Count; } }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly SlicePredicate predicate;
    }
}