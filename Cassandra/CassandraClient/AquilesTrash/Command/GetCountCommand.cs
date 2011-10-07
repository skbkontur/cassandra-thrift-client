using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class GetCountCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            var columnParent = BuildColumnParent();
            SlicePredicate slicePredicate = null;
            if(Predicate != null)
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            Count = cassandraClient.get_count(Key, columnParent, slicePredicate, GetCassandraConsistencyLevel());
        }

        public int Count { get; private set; }
        public AquilesSlicePredicate Predicate { get; set; }
    }
}