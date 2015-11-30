using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        private readonly byte[] columnName;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly byte[] rowKey;

        public GetCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, byte[] columnName)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.columnName = columnName;
        }

        public RawColumn Output { get; private set; }

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

            Output = columnOrSupercolumn != null ? columnOrSupercolumn.Column.FromCassandraColumn() : null;
        }
    }
}