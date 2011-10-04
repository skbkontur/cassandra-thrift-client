using System;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command.System
{
    /// <summary>
    /// Command to remove a Keyspace into a Cluster
    /// </summary>
    public class DropKeyspaceCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// Get or Set the keyspace
        /// </summary>
        public string Keyspace
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

        #region IAquilesCommand Members

        /// <summary>
        /// Executes a "system_drop_keyspace" over the connection.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            this.Output = cassandraClient.system_drop_keyspace(this.Keyspace);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            if (String.IsNullOrEmpty(this.Keyspace))
            {
                throw new AquilesCommandParameterException("Keyspace cannot be null or empty.");
            }
        }
        #endregion
    }
}
