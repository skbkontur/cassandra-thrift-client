using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command.System
{
    /// <summary>
    /// Command to udpate a ColumnFamily structure within an existent Keyspace
    /// </summary>
    public class UpdateColumnFamilyCommand : AbstractKeyspaceDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// Get or Set the ColumnFamilyDefinition
        /// </summary>
        public AquilesColumnFamily ColumnFamilyDefinition
        {
            get;
            set;
        }

        /// <summary>
        /// get the command output
        /// </summary>
        public string Output
        {
            get;
            private set;
        }

        #region IAquilesSystemCommand Members

        /// <summary>
        /// Executes a "system_update_column_family" over the connection, set the ClusterName property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            CfDef columnFamily = ModelConverterHelper.Convert<AquilesColumnFamily,CfDef>(this.ColumnFamilyDefinition);
            this.Output = cassandraClient.system_update_column_family(columnFamily);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            this.ColumnFamilyDefinition.ValidateForInsertOperation();
        }
        #endregion
    }
}
