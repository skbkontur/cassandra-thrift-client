using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using Vostok.Logging.Abstractions;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class MultiGetCountCommand : KeyspaceColumnFamilyDependantCommandBase, ISimpleCommand
    {
        public MultiGetCountCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate ?? new SlicePredicate(new SliceRange {Count = int.MaxValue});
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            var columnParent = BuildColumnParent();
            var slicePredicate = predicate.ToCassandraSlicePredicate();
            Output = new MultigetQueryHelpers(nameof(MultiGetCountCommand), keyspace, columnFamily, consistencyLevel)
                .EnumerateAllKeysWithPartialFetcher(
                    keys,
                    queryKeys => cassandraClient.multiget_count(queryKeys, columnParent, slicePredicate, consistencyLevel), logger);
        }

        public Dictionary<byte[], int> Output { get; private set; }
        public int QueriedPartitionsCount => keys.Count;

        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly SlicePredicate predicate;
    }
}