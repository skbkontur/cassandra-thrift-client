using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class InsertCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            logger.DebugFormat("Adding key '{0}' to columnFamily '{1}'.", Key, ColumnFamily);
            var column = ModelConverterHelper.Convert<AquilesColumn, Column>(Column);
            var columnParent = BuildColumnParent();
            cassandraClient.insert(Key, columnParent, column, GetCassandraConsistencyLevel());
        }

        public override void ValidateInput()
        {
            base.ValidateInput();
            if(Column == null)
                throw new AquilesCommandParameterException("Column parameter must have a value.");
            Column.ValidateForInsertOperation();
        }

        public AquilesColumn Column { set; private get; }
    }
}