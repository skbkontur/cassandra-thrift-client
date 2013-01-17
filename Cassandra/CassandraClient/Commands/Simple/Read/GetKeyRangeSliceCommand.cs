using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using KeyRange = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.KeyRange;
using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetKeyRangeSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetKeyRangeSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, KeyRange keyRange, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keyRange = keyRange;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var result = cassandraClient.get_range_slices(columnParent, predicate.ToCassandraSlicePredicate(), keyRange.ToCassandraKeyRange(), consistencyLevel);
            BuildOut(result);
        }

        public List<byte[]> Output { get; private set; }

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