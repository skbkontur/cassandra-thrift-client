using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to insert a Column into a Keyspace of a given cluster
    /// </summary>
    public class InsertCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand, IAquilesCommand
    {
        private ILog logger;
        /// <summary>
        /// get or set the name of the supercolumn
        /// </summary>
        public byte[] SuperColumn
        {
            set;
            get;
        }

        /// <summary>
        /// get or set the column information
        /// </summary>
        public AquilesColumn Column
        {
            set;
            get;
        }

        /// <summary>
        /// ctor
        /// </summary>
        public InsertCommand() : base()
        {
            this.logger = LogManager.GetLogger(this.GetType());
        }

        /// <summary>
        /// Executes a "insert" over the connection. No return values.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            logger.DebugFormat("Adding key '{0}' from columnFamily '{1}'.", this.Key, this.ColumnFamily);
            Column column = ModelConverterHelper.Convert<AquilesColumn,Column>(this.Column);
            ColumnParent columnParent = this.BuildColumnParent(this.SuperColumn);
            cassandraClient.insert(this.Key, columnParent, column, this.GetCassandraConsistencyLevel());

        }
        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();
            if (this.Column == null)
            {
                throw new AquilesCommandParameterException("Column parameter must have a value.");
            }
            this.Column.ValidateForInsertOperation();
        }
        

    }
}
