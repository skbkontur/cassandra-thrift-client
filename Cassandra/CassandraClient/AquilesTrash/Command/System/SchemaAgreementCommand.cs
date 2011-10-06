using System.Collections.Generic;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command.System
{
    /// <summary>
    /// Command to retrieve node agreements over keyspaces schemas
    /// </summary>
    public class SchemaAgreementCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// get Dictionary of Keyspace agreements where key is the version (higher versions are the new ones) of the schemas and the value is the list of nodes that agreeds.
        /// </summary>
        public Dictionary<string, List<string>> Output
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "describe_schema_versions" over the connection.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            this.Output = null;
            Dictionary<string,List<string>> keyspaceAgreement = cassandraClient.describe_schema_versions();
            this.BuildOutput(keyspaceAgreement);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            // DO NOTHING
        }

        

        private void BuildOutput(Dictionary<string, List<string>> keyspaceAgreement)
        {
            this.Output = keyspaceAgreement;
        }
    }
}
