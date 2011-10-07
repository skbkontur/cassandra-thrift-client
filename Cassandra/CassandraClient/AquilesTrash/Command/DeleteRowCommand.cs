using Apache.Cassandra;

using CassandraClient.Core;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to delete a Column, SuperColumn or a Key from Keyspace of a given cluster
    /// </summary>
    public class DeleteRowCommand : AbstractKeyspaceDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// ctor
        /// </summary>
        public DeleteRowCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        /// <summary>
        /// Executes a "remove" over the connection. No return values
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public override void Execute(Cassandra.Client cassandraClient)
        {
            ColumnPath columnPath;
            logger.DebugFormat("Removing key '{0}' from columnFamily '{1}'.", Key, ColumnFamily);
            var timestamp = Timestamp ?? DateTimeService.UtcNow.Ticks;
            cassandraClient.remove(
                Key,
                new ColumnPath
                    {
                        Column_family = ColumnFamily
                    },
                timestamp,
                GetCassandraConsistencyLevel());
        }

        public override void ValidateInput()
        {
        }

        

        /// <summary>
        /// get or set supercolumn name
        /// </summary>
        public byte[] Key { get; set; }

        public string ColumnFamily { get; set; }
        public long? Timestamp { get; set; }
        private readonly ILog logger;
    }
}