using System.Collections.Generic;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    public class MultiGetCountCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public MultiGetCountCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, AquilesSlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            SlicePredicate slicePredicate;
            if (predicate != null)
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
            Output = cassandraClient.multiget_count(keys, BuildColumnParent(), slicePredicate, consistencyLevel);
        }

        public Dictionary<byte[], int> Output { get; private set; }
        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly AquilesSlicePredicate predicate;
    }
}