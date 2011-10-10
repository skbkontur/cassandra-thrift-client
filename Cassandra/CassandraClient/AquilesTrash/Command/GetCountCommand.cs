using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class GetCountCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
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