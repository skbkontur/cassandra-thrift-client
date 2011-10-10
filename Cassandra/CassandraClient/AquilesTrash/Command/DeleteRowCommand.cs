using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class DeleteRowCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            logger.Debug("Removing key '{0}' from columnFamily '{1}'.", Key, ColumnFamily);
            var timestamp = Timestamp ?? DateTimeService.UtcNow.Ticks;
            cassandraClient.remove(
                Key,
                new ColumnPath {Column_family = ColumnFamily},
                timestamp,
                GetCassandraConsistencyLevel());
        }

        public long? Timestamp { private get; set; }
    }
}