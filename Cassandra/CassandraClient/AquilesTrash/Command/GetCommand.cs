using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Model;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    public class GetCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public GetCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        public override void Execute(Cassandra.Client cassandraClient)
        {
            ColumnOrSuperColumn columnOrSupercolumn = null;
            ColumnPath columnPath = BuildColumnPath(ColumnName);
            try
            {
                columnOrSupercolumn = cassandraClient.get(Key, columnPath, GetCassandraConsistencyLevel());
            }
            catch(NotFoundException ex)
            {
                var message = string.Format("{{ Key: '{0}', ColumnFamily: '{1}', Column: '{2}' }} not found.", Key, ColumnFamily, ColumnName);
                logger.Warn(message, ex);
            }

            if(columnOrSupercolumn != null)
                Output = ModelConverterHelper.Convert<AquilesColumn, Column>(columnOrSupercolumn.Column);
            else
                Output = null;
        }

        public AquilesColumn Output { get; private set; }
        public byte[] ColumnName { set; private get; }

        private readonly ILog logger;
    }
}