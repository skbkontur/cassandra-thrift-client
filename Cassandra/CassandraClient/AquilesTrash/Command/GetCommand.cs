using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class GetCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public GetCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
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