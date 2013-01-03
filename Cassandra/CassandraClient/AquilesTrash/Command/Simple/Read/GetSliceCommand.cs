using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    public class GetSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetSliceCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, AquilesSlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            ColumnParent columnParent = BuildColumnParent();
            SlicePredicate apachePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(predicate);
            List<ColumnOrSuperColumn> output = cassandraClient.get_slice(rowKey, columnParent, apachePredicate, consistencyLevel);
            BuildOut(output);
        }

        public List<AquilesColumn> Output { get; private set; }

        private void BuildOut(IEnumerable<ColumnOrSuperColumn> output)
        {
            Output = output.Select(x => x.Column)
                .Select(ModelConverterHelper.Convert<AquilesColumn, Column>)
                .ToList();
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly AquilesSlicePredicate predicate;
    }
}