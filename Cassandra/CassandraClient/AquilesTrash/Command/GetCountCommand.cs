using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class GetCountCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            ColumnParent columnParent = BuildColumnParent();
            SlicePredicate slicePredicate;
            if(Predicate != null)
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            else
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(new AquilesSlicePredicate
                    {
                        SliceRange = new AquilesSliceRange
                            {
                                Count = int.MaxValue,
                            }
                    });
            Count = cassandraClient.get_count(Key, columnParent, slicePredicate, GetCassandraConsistencyLevel());
        }

        public int Count { get; private set; }
        public AquilesSlicePredicate Predicate { get; set; }
    }
}