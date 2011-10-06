using CassandraClient.AquilesTrash.Exceptions;

using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve the partitioner for a cluster
    /// </summary>
    public class RetrieveClusterPartitionerCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// get the configured Partitioner
        /// </summary>
        public string Partitioner
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "describe_partitioner" over the connection, set the Partitioner property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            string partitioner = cassandraClient.describe_partitioner();
            this.Partitioner = partitioner;
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
