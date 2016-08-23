using Apache.Cassandra;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Read
{
    internal class GetCommand : KeyspaceColumnFamilyDependantCommandBase, ISinglePartitionQuery, ISimpleCommand 
    {
        public GetCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, byte[] columnName)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.columnName = columnName;
        }

        [NotNull]
        public string PartitionKey { get { return StringExtensions.BytesToString(rowKey); } }
        public int QueriedPartitionsCount { get { return 1; } }

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

        public RawColumn Output { get; private set; }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly byte[] columnName;
    }
}