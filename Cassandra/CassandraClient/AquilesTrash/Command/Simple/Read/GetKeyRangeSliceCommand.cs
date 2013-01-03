using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    public class GetKeyRangeSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetKeyRangeSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, AquilesKeyRange keyTokenRange, AquilesSlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keyTokenRange = keyTokenRange;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var apachePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(predicate);
            var keyRange = ModelConverterHelper.Convert<AquilesKeyRange, KeyRange>(keyTokenRange);
            var result = cassandraClient.get_range_slices(columnParent, apachePredicate, keyRange, consistencyLevel);
            BuildOut(result);
        }

        public List<byte[]> Output { get; private set; }

        private void BuildOut(IEnumerable<KeySlice> output)
        {
            var returnObjs = output.Select(keySlice => keySlice.Key).ToList();
            Output = returnObjs;
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly AquilesKeyRange keyTokenRange;
        private readonly AquilesSlicePredicate predicate;
    }
}