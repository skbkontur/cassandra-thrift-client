using System.Collections.Generic;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class MultiGetCountCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
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