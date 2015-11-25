using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;
using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetSliceCommand<T> : KeyspaceColumnFamilyDependantCommandBase where T : class, IColumn, new()
    {
        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
        private readonly byte[] rowKey;

        public GetSliceCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
        }

        public List<T> Output { get; private set; }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var output = cassandraClient.get_slice(rowKey, columnParent, predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOut(output);
        }

        private void BuildOut(IEnumerable<ColumnOrSuperColumn> output)
        {
            Output = output.Select(x => x.Column).Select(x => x.FromCassandraColumn<T>()).ToList();
        }
    }
}