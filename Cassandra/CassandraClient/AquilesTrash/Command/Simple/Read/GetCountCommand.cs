using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    public class GetCountCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetCountCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, AquilesSlicePredicate predicate = null)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            ColumnParent columnParent = BuildColumnParent();
            SlicePredicate slicePredicate;
            if(predicate != null)
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(predicate);
            else
            {
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(new AquilesSlicePredicate
                    {
                        SliceRange = new AquilesSliceRange
                            {
                                Count = int.MaxValue,
                            }
                    });
            }
            Count = cassandraClient.get_count(rowKey, columnParent, slicePredicate, consistencyLevel);
        }

        public int Count { get; private set; }
        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly AquilesSlicePredicate predicate;
    }
}