using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class GetCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            ColumnOrSuperColumn columnOrSupercolumn = null;
            ColumnPath columnPath = BuildColumnPath(ColumnName);
            try
            {
                columnOrSupercolumn = cassandraClient.get(Key, columnPath, GetCassandraConsistencyLevel());
            }
            catch(NotFoundException ex)
            {
                //ничего не делаем
            }

            if(columnOrSupercolumn != null)
                Output = ModelConverterHelper.Convert<AquilesColumn, Column>(columnOrSupercolumn.Column);
            else
                Output = null;
        }

        public AquilesColumn Output { get; private set; }
        public byte[] ColumnName { set; private get; }

        private string ByteArrayAsString(IEnumerable<byte> arr)
        {
            return string.Join(",", arr.Select(x => x.ToString()));
        }
    }
}