using System;
using System.Collections.Generic;

using CassandraClient.AquilesTrash.Model;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve Keyspace structure from cluster.
    /// </summary>
    public class DescribeKeyspaceCommand : CassandraClient.AquilesTrash.Command.AbstractCommand, IAquilesCommand
    {
        private const string COLUMN_TYPE = "Type";

        /// <summary>
        /// get or set the Keyspace
        /// </summary>
        public string Keyspace
        {
            set;
            protected get;
        }

        /// <summary>
        /// get Dictionary of ColumnFamilies where key is the name of the ColumnFamily
        /// </summary>
        public AquilesKeyspace KeyspaceInformation
        {
            get;
            private set;
        }

        #region IAquilesCommand Members
        /// <summary>
        /// Executes a "describe_keyspace" over the connection.
        /// 
        /// Note: This command is not yet finished.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            KsDef keyspaceDescription = cassandraClient.describe_keyspace(this.Keyspace);

            this.KeyspaceInformation = ModelConverterHelper.Convert<AquilesKeyspace, KsDef>(keyspaceDescription);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            //Do nothing

        }
        #endregion

        private AquilesColumnFamily Translate(string columnFamilyName, Dictionary<string, string> data)
        {
            string rawType = null;
            AquilesColumnFamilyType type = AquilesColumnFamilyType.Standard;
            if (data.TryGetValue(COLUMN_TYPE, out rawType))
            {
                type = (AquilesColumnFamilyType)Enum.Parse(typeof(AquilesColumnFamilyType), rawType);
            }
            AquilesColumnFamily columnFamily = new AquilesColumnFamily();
            columnFamily.Name = columnFamilyName;
            columnFamily.Type = type;

            return columnFamily;
        }
    }
}
