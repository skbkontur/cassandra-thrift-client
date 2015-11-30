using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write
{
    internal class InsertCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        private readonly RawColumn column;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly byte[] rowKey;

        public InsertCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, RawColumn column)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.column = column;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.insert(rowKey, BuildColumnParent(), column.ToCassandraColumn(), consistencyLevel);
        }
    }
}