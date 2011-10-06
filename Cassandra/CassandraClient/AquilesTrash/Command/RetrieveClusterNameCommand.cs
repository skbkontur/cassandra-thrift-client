using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve the ClusterName from a Cluster (the real name)
    /// </summary>
    public class RetrieveClusterNameCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// Get the ClusterName
        /// </summary>
        public string ClusterName
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "describe_cluster_name" over the connection, set the ClusterName property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            this.ClusterName = cassandraClient.describe_cluster_name();
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
