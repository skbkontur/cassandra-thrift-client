using System.Collections.Generic;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class MultiGetCountCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
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
            Output = cassandraClient.multiget_count(Keys, BuildColumnParent(), slicePredicate, GetCassandraConsistencyLevel());
        }

        public List<byte[]> Keys { private get; set; }
        public Dictionary<byte[], int> Output { get; private set; }
        public AquilesSlicePredicate Predicate { private get; set; }
    }
}