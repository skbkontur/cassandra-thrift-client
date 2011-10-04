using System.Globalization;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter;

using log4net;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve a Column or SuperColumn from Keyspace of a given cluster with the given key
    /// </summary>
    public class GetCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// ctor
        /// </summary>
        public GetCommand()
        {
            logger = LogManager.GetLogger(GetType());
        }

        #region IAquilesCommand Members

        /// <summary>
        /// Executes a "get" over the connection. Return values are set into Output
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            ColumnOrSuperColumn columnOrSupercolumn = null;
            ColumnPath columnPath = BuildColumnPath(ColumnFamily, SuperColumnName, ColumnName);
            try
            {
                columnOrSupercolumn = cassandraClient.get(Key, columnPath, GetCassandraConsistencyLevel());
            }
            catch(NotFoundException ex)
            {
                logger.Warn(
                    string.Format(CultureInfo.InvariantCulture,
                                  "{{ Key: '{0}', ColumnFamily: '{1}', SuperColumn: '{2}', Column: '{3}' }} not found.",
                                  Key,
                                  ColumnFamily,
                                  SuperColumnName,
                                  ColumnName),
                    ex);
            }

            if(columnOrSupercolumn != null)
                buildOut(columnOrSupercolumn);
            else
            {
                // in case of reuse of the command
                Output = null;
            }
        }

        #endregion

        /// <summary>
        /// get the return value
        /// </summary>
        public Out Output { get; private set; }

        /// <summary>
        /// get or set the SuperColumn
        /// </summary>
        public byte[] SuperColumnName { set; get; }

        /// <summary>
        /// get or set the ColumnName
        /// </summary>
        public byte[] ColumnName { set; get; }

        /// <summary>
        /// structure to Return Values
        /// </summary>
        public class Out
        {
            /// <summary>
            /// get or set Column Information
            /// </summary>
            public AquilesColumn Column { get; set; }

            /// <summary>
            /// get or set SuperColumn Information
            /// </summary>
            public AquilesSuperColumn SuperColumn { get; set; }
        }

        private void buildOut(ColumnOrSuperColumn columnOrSupercolumn)
        {
            var outResponse = new Out();
            outResponse.Column = ModelConverterHelper.Convert<AquilesColumn, Column>(columnOrSupercolumn.Column);
            outResponse.SuperColumn = ModelConverterHelper.Convert<AquilesSuperColumn, SuperColumn>(columnOrSupercolumn.Super_column);
            Output = outResponse;
        }

        private readonly ILog logger;
    }
}