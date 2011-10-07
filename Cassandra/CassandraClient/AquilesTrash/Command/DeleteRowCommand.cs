using Apache.Cassandra;

using CassandraClient.Core;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    public class DeleteRowCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public DeleteRowCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        public override void Execute(Cassandra.Client cassandraClient)
        {
            logger.DebugFormat("Removing key '{0}' from columnFamily '{1}'.", Key, ColumnFamily);
            var timestamp = Timestamp ?? DateTimeService.UtcNow.Ticks;
            cassandraClient.remove(
                Key,
                new ColumnPath {Column_family = ColumnFamily},
                timestamp,
                GetCassandraConsistencyLevel());
        }

        public long? Timestamp { private get; set; }
        private readonly ILog logger;
    }
}