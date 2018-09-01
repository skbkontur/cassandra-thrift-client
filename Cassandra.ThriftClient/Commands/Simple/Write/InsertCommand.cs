using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write
{
    internal class InsertCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand
    {
        public InsertCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, RawColumn column)
            : base(keyspace, columnFamily)
        {
            PartitionKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.column = column;
        }

        [NotNull]
        public byte[] PartitionKey { get; }

        public int QueriedPartitionsCount => 1;

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.insert(PartitionKey, BuildColumnParent(), column.ToCassandraColumn(), consistencyLevel);
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly RawColumn column;
    }
}