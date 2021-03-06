using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Abstractions.Internal;
using SkbKontur.Cassandra.ThriftClient.Commands.Base;

using Vostok.Logging.Abstractions;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;
using KeyRange = SkbKontur.Cassandra.ThriftClient.Abstractions.Internal.KeyRange;
using SlicePredicate = SkbKontur.Cassandra.ThriftClient.Abstractions.Internal.SlicePredicate;

namespace SkbKontur.Cassandra.ThriftClient.Commands.Simple.Read
{
    internal class GetKeyRangeSliceCommand : KeyspaceColumnFamilyDependantCommandBase, ISimpleCommand
    {
        public GetKeyRangeSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, KeyRange keyRange, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keyRange = keyRange;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var result = cassandraClient.get_range_slices(columnParent, predicate.ToCassandraSlicePredicate(), keyRange.ToCassandraKeyRange(), consistencyLevel);
            BuildOut(result);
        }

        public List<byte[]> Output { get; private set; }
        public int QueriedPartitionsCount => Output.Count;
        public long? ResponseSize => Output.Sum(x => (long)x.Length);

        private void BuildOut(IEnumerable<KeySlice> output)
        {
            var returnObjs = output.Select(keySlice => keySlice.Key).ToList();
            Output = returnObjs;
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly KeyRange keyRange;
        private readonly SlicePredicate predicate;
    }
}