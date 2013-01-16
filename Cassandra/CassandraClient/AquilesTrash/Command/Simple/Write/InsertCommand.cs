using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

using Column = SKBKontur.Cassandra.CassandraClient.Abstractions.Column;
using ColumnExtensions = SKBKontur.Cassandra.CassandraClient.Abstractions.ColumnExtensions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Write
{
    public class InsertCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public InsertCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, Column column)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.column = column;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.insert(rowKey, BuildColumnParent(), ColumnExtensions.ToCassandraColumn(column), consistencyLevel);
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly Column column;
    }
}