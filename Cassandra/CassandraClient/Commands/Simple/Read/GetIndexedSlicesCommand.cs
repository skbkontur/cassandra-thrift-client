using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using IndexClause = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.IndexClause;
using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetIndexedSlicesCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetIndexedSlicesCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, SlicePredicate predicate, IndexClause indexClause)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
            this.indexClause = indexClause;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var columnParent = BuildColumnParent();
            var result = cassandraClient.get_indexed_slices(columnParent, indexClause.ToCassandraIndexClause(), predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOutput(result);
        }

        public List<byte[]> Output { private set; get; }
        public override int QueriedPartitionsCount { get { return Output.Count; } }

        private void BuildOutput(IEnumerable<KeySlice> result)
        {
            var returnObjs = result.Select(keySlice => keySlice.Key).ToList();
            Output = returnObjs;
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
        private readonly IndexClause indexClause;
    }
}