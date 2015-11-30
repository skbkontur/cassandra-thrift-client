using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;
using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class MultiGetSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly SlicePredicate predicate;

        public MultiGetSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate;
        }

        public Dictionary<byte[], List<RawColumn>> Output { get; private set; }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var output = cassandraClient.multiget_slice(keys, BuildColumnParent(), predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOut(output);
        }

        private void BuildOut(Dictionary<byte[], List<ColumnOrSuperColumn>> output)
        {
            Output = new Dictionary<byte[], List<RawColumn>>();
            foreach(var outputKeyValuePair in output)
            {
                var columnOrSuperColumnList = outputKeyValuePair.Value.Select(x => x.Column)
                                                                .Select(x => x.FromCassandraColumn()).ToList();
                Output.Add(outputKeyValuePair.Key, columnOrSuperColumnList);
            }
        }
    }
}