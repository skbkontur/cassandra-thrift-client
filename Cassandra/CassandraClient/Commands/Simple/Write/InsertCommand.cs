using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write
{
    internal class InsertCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand
    {
        public InsertCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, RawColumn column)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.column = column;
        }

        [NotNull]
        public string PartitionKey { get { return StringExtensions.BytesToString(rowKey); } }
        public int QueriedPartitionsCount { get { return 1; } }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.insert(rowKey, BuildColumnParent(), column.ToCassandraColumn(), consistencyLevel);
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly RawColumn column;
    }
}