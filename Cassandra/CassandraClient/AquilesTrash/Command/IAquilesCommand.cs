using Apache.Cassandra;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Interface for any AquilesCommand with minimum methods to operate with.
    /// </summary>
    public interface IAquilesCommand
    {
        /// <summary>
        /// Execute the command over the opened thrift client
        /// </summary>
        /// <param name="cassandraClient">an opened thrift Client</param>
        void Execute(Cassandra.Client cassandraClient);

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        void ValidateInput();

        /// <summary>
        /// Indicates if this command applies only to a keyspace
        /// </summary>
        bool isKeyspaceDependant
        {
            get;
        }
    }
}
