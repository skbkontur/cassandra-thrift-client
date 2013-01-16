using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

using Column = SKBKontur.Cassandra.CassandraClient.Abstractions.Column;
using ColumnExtensions = SKBKontur.Cassandra.CassandraClient.Abstractions.ColumnExtensions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    public class GetCommand : KeyspaceColumnFamilyDependantCommandBase
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
            ColumnPath columnPath = BuildColumnPath(columnName);
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

        private string ByteArrayAsString(IEnumerable<byte> arr)
        {
            return string.Join(",", arr.Select(x => x.ToString()));
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly byte[] columnName;
    }
}