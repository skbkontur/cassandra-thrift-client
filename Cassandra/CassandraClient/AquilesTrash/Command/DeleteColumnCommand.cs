using Apache.Cassandra;

using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;
using CassandraClient.Core;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to delete a Column, SuperColumn or a Key from Keyspace of a given cluster
    /// </summary>
    public class DeleteColumnCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// ctor
        /// </summary>
        public DeleteColumnCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        #region IAquilesCommand Members

        /// <summary>
        /// Executes a "remove" over the connection. No return values
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            ColumnPath columnPath;
            logger.DebugFormat("Removing column '{0}' for key '{1}' from columnFamily '{2}'.", Column.ColumnName, Key, ColumnFamily);
            columnPath = BuildColumnPath(ColumnFamily, SuperColumnName, Column.ColumnName);
            var timestamp = Column.Timestamp.HasValue ? Column.Timestamp.Value : DateTimeService.UtcNow.Ticks;
            cassandraClient.remove(Key, columnPath, timestamp, GetCassandraConsistencyLevel());
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();
            if(Column != null)
            {
                Column.ValidateForDeletationOperation();
                if(!Column.Timestamp.HasValue)
                    throw new AquilesCommandParameterException("If input Column exists, must have a valid Timestamp.");
            }
        }

        #endregion

        /// <summary>
        /// get or set supercolumn name
        /// </summary>
        public byte[] SuperColumnName { set; get; }

        /// <summary>
        /// get or set Column information
        /// </summary>
        public AquilesColumn Column { set; get; }
        private readonly ILog logger;
    }
}