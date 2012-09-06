using System.Diagnostics;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Core;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class DeleteRowCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var timestamp = Timestamp ?? DateTimeService.UtcNow.Ticks;
	    var stopwatch = Stopwatch.StartNew();
            cassandraClient.remove(
                Key,
                new ColumnPath {Column_family = ColumnFamily},
                timestamp,
                GetCassandraConsistencyLevel());

		logger.DebugFormat("Removed key '{0}' from columnFamily '{1}' in {2}ms.", Key, ColumnFamily, stopwatch.ElapsedMilliseconds);
        }

        public long? Timestamp { private get; set; }
    }
}