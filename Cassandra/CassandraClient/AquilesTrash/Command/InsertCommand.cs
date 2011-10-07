using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    public class InsertCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public InsertCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        public override void Execute(Cassandra.Client cassandraClient)
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
        private readonly ILog logger;
    }
}