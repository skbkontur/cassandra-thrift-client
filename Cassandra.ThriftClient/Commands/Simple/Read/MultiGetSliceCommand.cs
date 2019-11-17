using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Abstractions.Internal;
using SkbKontur.Cassandra.ThriftClient.Commands.Base;
using SkbKontur.Cassandra.ThriftClient.Helpers;

using Vostok.Logging.Abstractions;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;
using SlicePredicate = SkbKontur.Cassandra.ThriftClient.Abstractions.Internal.SlicePredicate;

namespace SkbKontur.Cassandra.ThriftClient.Commands.Simple.Read
{
    internal class MultiGetSliceCommand : KeyspaceColumnFamilyDependantCommandBase, ISimpleCommand
    {
        public MultiGetSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            var columnParent = BuildColumnParent();
            var slicePredicate = predicate.ToCassandraSlicePredicate();
            var output = new MultigetQueryHelper(nameof(MultiGetSliceCommand), keyspace, columnFamily, consistencyLevel)
                .EnumerateAllKeysWithPartialFetcher(
                    keys,
                    queryKeys => cassandraClient.multiget_slice(queryKeys, columnParent, slicePredicate, consistencyLevel), logger);
            BuildOut(output);
        }

        public Dictionary<byte[], List<RawColumn>> Output { get; private set; }
        public int QueriedPartitionsCount => keys.Count;
        public long? ResponseSize => Output.Sum(kvp => kvp.Value.Sum(x => (long)x.Value.Length));

        private void BuildOut(Dictionary<byte[], List<ColumnOrSuperColumn>> output)
        {
            Output = new Dictionary<byte[], List<RawColumn>>();
            foreach (var outputKeyValuePair in output)
            {
                var columnOrSuperColumnList = outputKeyValuePair.Value.Select(x => x.Column)
                                                                .Select(x => x.FromCassandraColumn()).ToList();
                Output.Add(outputKeyValuePair.Key, columnOrSuperColumnList);
            }
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly SlicePredicate predicate;
    }
}