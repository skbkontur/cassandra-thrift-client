using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve the Cassandra version from a Cluster (the real name)
    /// </summary>
    public class RetrieveClusterVersionCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// Get the Cassandra version
        /// </summary>
        public string Version
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "describe_version" over the connection, set the Version property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            this.Version = cassandraClient.describe_version();
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            //do nothing
        }

        
    }
}
