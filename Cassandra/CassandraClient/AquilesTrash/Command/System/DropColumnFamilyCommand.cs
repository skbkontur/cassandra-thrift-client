using System;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command.System
{
    /// <summary>
    /// Command to remove a ColumnFamily within a Keyspace into a Cluster
    /// </summary>
    public class DropColumnFamilyCommand : AbstractKeyspaceDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// Get or Set the ColumnFamily
        /// </summary>
        public string ColumnFamily
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

        /// <summary>
        /// Executes a "system_drop_column_family" over the connection.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            this.Output = cassandraClient.system_drop_column_family(this.ColumnFamily);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            if (String.IsNullOrEmpty(this.ColumnFamily))
            {
                throw new AquilesCommandParameterException("ColumnFamily cannot be null or empty.");
            }
        }
        
    }
}
