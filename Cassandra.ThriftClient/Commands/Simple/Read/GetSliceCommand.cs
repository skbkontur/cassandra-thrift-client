using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Vostok.Logging.Abstractions;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;
using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetSliceCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand
    {
        public GetSliceCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            PartitionKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.predicate = predicate;
        }

        [NotNull]
        public byte[] PartitionKey { get; }

        public int QueriedPartitionsCount => 1;
        public long? ResponseSize => Output.Sum(x => (long)x.Value.Length);

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = null;
            var columnParent = BuildColumnParent();
            var output = cassandraClient.get_slice(PartitionKey, columnParent, predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOut(output);
        }

        public List<RawColumn> Output { get; private set; }

        private void BuildOut(IEnumerable<ColumnOrSuperColumn> output)
        {
            Output = output.Select(x => x.Column).Select(x => x.FromCassandraColumn()).ToList();
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly SlicePredicate predicate;
    }
}