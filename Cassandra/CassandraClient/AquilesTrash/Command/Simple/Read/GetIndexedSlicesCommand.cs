using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    internal class GetIndexedSlicesCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetIndexedSlicesCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, SlicePredicate predicate, AquilesIndexClause indexClause)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
            this.indexClause = indexClause;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var columnParent = BuildColumnParent();
            var apacheIndexClause = ModelConverterHelper.Convert<AquilesIndexClause, IndexClause>(indexClause);
            var result = cassandraClient.get_indexed_slices(columnParent, apacheIndexClause, predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOutput(result);
        }

        public List<byte[]> Output { private set; get; }

        private void BuildOutput(IEnumerable<KeySlice> result)
        {
            var returnObjs = result.Select(keySlice => keySlice.Key).ToList();
            Output = returnObjs;
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
        private readonly AquilesIndexClause indexClause;
    }
}