using System.Collections.Generic;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

using Apache.Cassandra;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve the list of keyspaces for a cluster
    /// </summary>
    public class RetrieveKeyspacesCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        /// <summary>
        /// get the list of keyspaces
        /// </summary>
        public List<AquilesKeyspace> Keyspaces
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "describe_keyspaces" over the connection, set the Keyspaces property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            List<KsDef> keySpaces = cassandraClient.describe_keyspaces();
            this.Keyspaces = this.buildKeyspaces(keySpaces);
        }

        private List<AquilesKeyspace> buildKeyspaces(List<KsDef> keySpaces)
        {
            List<AquilesKeyspace> convertedKeyspaces = null;
            if (keySpaces != null)
            {
                convertedKeyspaces = new List<AquilesKeyspace>(keySpaces.Count);
                foreach (KsDef keyspace in keySpaces)
                {
                    convertedKeyspaces.Add(ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(keyspace));
                }
            }
            else
            {
                convertedKeyspaces = null;
            }
            return convertedKeyspaces;
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
