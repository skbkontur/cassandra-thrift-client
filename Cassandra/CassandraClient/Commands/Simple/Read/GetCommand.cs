using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using Column = SKBKontur.Cassandra.CassandraClient.Abstractions.Column;
using ColumnExtensions = SKBKontur.Cassandra.CassandraClient.Abstractions.ColumnExtensions;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public GetCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, byte[] columnName)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.columnName = columnName;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            ColumnOrSuperColumn columnOrSupercolumn = null;
            var columnPath = BuildColumnPath(columnName);
            try
            {
                columnOrSupercolumn = cassandraClient.get(rowKey, columnPath, consistencyLevel);
            }
            catch(NotFoundException)
            {
                //ничего не делаем
            }

            Output = columnOrSupercolumn != null ? ColumnExtensions.FromCassandraColumn(columnOrSupercolumn.Column) : null;
        }

        public Column Output { get; private set; }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly byte[] columnName;
    }
}