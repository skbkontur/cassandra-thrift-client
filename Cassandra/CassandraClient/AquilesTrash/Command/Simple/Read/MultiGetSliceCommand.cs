using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    public class MultiGetSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public MultiGetSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, AquilesSlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var apachePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(predicate);
            var output = cassandraClient.multiget_slice(keys, columnParent, apachePredicate, consistencyLevel);
            BuildOut(output);
        }

        public Dictionary<byte[], List<AquilesColumn>> Output { get; private set; }

        private void BuildOut(Dictionary<byte[], List<ColumnOrSuperColumn>> output)
        {
            Output = new Dictionary<byte[], List<AquilesColumn>>();
            foreach(var outputKeyValuePair in output)
            {
                var columnOrSuperColumnList = outputKeyValuePair.Value.Select(x => x.Column)
                    .Select(ModelConverterHelper.Convert<AquilesColumn, Column>)
                    .ToList();
                Output.Add(outputKeyValuePair.Key, columnOrSuperColumnList);
            }
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly AquilesSlicePredicate predicate;
    }
}