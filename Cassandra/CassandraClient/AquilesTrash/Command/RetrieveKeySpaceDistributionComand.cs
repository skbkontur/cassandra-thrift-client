using System;
using System.Collections.Generic;

using CassandraClient.AquilesTrash.Model;
using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve the keyspace's token distribution over the nodes from a Cluster (the real name)
    /// </summary>
    public class RetrieveKeyspaceDistributionComand : AbstractKeyspaceDependantCommand, IAquilesCommand
    {

        /// <summary>
        /// get or set the Keyspace
        /// </summary>
        public string Keyspace
        {
            set;
            protected get;
        }

        /// <summary>
        /// Get the TokenRanges information
        /// </summary>
        public List<AquilesTokenRange> Output
        {
            get;
            private set;
        }

        #region IAquilesCommand Members

        /// <summary>
        /// Executes a "describe_ring" over the connection, set the Version property with the returned value.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            List<TokenRange> results = cassandraClient.describe_ring(this.Keyspace);
            this.BuildOut(results);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            if (String.IsNullOrEmpty(this.Keyspace))
            {
                throw new AquilesCommandParameterException("Keyspace must be not null or empty.");
            }
        }
        #endregion

        private void BuildOut(List<TokenRange> results)
        {
            if (results != null)
            {
                this.Output = new List<AquilesTokenRange>();
                foreach (TokenRange tokenRange in results)
                {
                    this.Output.Add(ModelConverterHelper.Convert<AquilesTokenRange, TokenRange>(tokenRange));    
                }
            }
        }
    }
}
