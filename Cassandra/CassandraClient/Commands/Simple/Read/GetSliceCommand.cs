using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Column = SKBKontur.Cassandra.CassandraClient.Abstractions.Column;
using ColumnExtensions = SKBKontur.Cassandra.CassandraClient.Abstractions.ColumnExtensions;
using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetSliceCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var output = cassandraClient.get_slice(rowKey, columnParent, predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOut(output);
        }

        public List<Column> Output { get; private set; }

        private void BuildOut(IEnumerable<ColumnOrSuperColumn> output)
        {
            Output = output.Select(x => x.Column).Select(ColumnExtensions.FromCassandraColumn).ToList();
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
    }
}