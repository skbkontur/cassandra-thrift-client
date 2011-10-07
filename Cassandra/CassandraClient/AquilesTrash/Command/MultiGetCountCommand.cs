using System.Collections.Generic;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command
{
    public class MultiGetCountCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            SlicePredicate slicePredicate = null;
            if(Predicate != null)
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(Predicate);
            Output = cassandraClient.multiget_count(Keys, BuildColumnParent(), slicePredicate, GetCassandraConsistencyLevel());
        }

        public List<byte[]> Keys { private get; set; }
        public Dictionary<byte[], int> Output { get; private set; }
        public AquilesSlicePredicate Predicate { private get; set; }
    }
}