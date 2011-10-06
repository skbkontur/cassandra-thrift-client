using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.AquilesTrash.Command.System
{
    /// <summary>
    /// Command to add a Keyspace into a Cluster
    /// </summary>
    public class AddKeyspaceCommand : AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// Get or Set the KeyspaceDefinition
        /// </summary>
        public AquilesKeyspace KeyspaceDefinition
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
        /// Executes a "system_add_keyspace" over the connection, set the ClusterName property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            KsDef keyspace = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(this.KeyspaceDefinition);
            this.Output = cassandraClient.system_add_keyspace(keyspace);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            this.KeyspaceDefinition.ValidateForInsertOperation();
        }

        
    }
}
