using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.Core;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to delete a Column, SuperColumn or a Key from Keyspace of a given cluster
    /// </summary>
    public class DeleteCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand, IAquilesCommand
    {
        private ILog logger;

        /// <summary>
        /// get or set supercolumn name
        /// </summary>
        public byte[] SuperColumnName
        {
            set;
            get;
        }
        /// <summary>
        /// get or set Column information
        /// </summary>
        public AquilesColumn Column
        {
            set;
            get;
        }

        /// <summary>
        /// ctor
        /// </summary>
        public DeleteCommand() : base()
        {
            this.logger = LogManager.GetLogger(GetType());
        }

        #region IAquilesCommand Members
        /// <summary>
        /// Executes a "remove" over the connection. No return values
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            ColumnPath columnPath;
            if (this.Column != null)
            {
                logger.DebugFormat("Removing column '{0}' for key '{1}' from columnFamily '{2}'.", this.Column.ColumnName, this.Key, this.ColumnFamily);
                columnPath = this.BuildColumnPath(this.ColumnFamily, this.SuperColumnName, this.Column.ColumnName);
                cassandraClient.remove(this.Key, columnPath, this.Column.Timestamp.Value, this.GetCassandraConsistencyLevel());
            }
            else
            {
                logger.DebugFormat("Removing key '{0}' from columnFamily '{1}'.", Key, ColumnFamily);
                columnPath = BuildColumnPath(ColumnFamily, SuperColumnName, null);
                cassandraClient.remove(Key, columnPath, DateTimeService.UtcNow.Ticks, GetCassandraConsistencyLevel());
            }
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();
            if (this.Column != null)
            {
                this.Column.ValidateForDeletationOperation();
                if (!this.Column.Timestamp.HasValue)
                {
                    throw new AquilesCommandParameterException("If input Column exists, must have a valid Timestamp.");
                }
            }
        }
        #endregion
    }
}
