using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

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

            if(columnOrSupercolumn != null)
                Output = ModelConverterHelper.Convert<AquilesColumn, Column>(columnOrSupercolumn.Column);
            else
                Output = null;
        }

        public AquilesColumn Output { get; private set; }

        private string ByteArrayAsString(IEnumerable<byte> arr)
        {
            return string.Join(",", arr.Select(x => x.ToString()));
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly byte[] columnName;
    }
}