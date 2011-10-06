using System.Collections.Generic;

using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve a Column or SuperColumn from Keyspace of a given cluster with the given key
    /// </summary>
    public class MultiGetCountCommand : AbstractKeyspaceColumnFamilyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set the list of keys
        /// </summary>
        public List<byte[]> Keys
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the SuperColumn
        /// </summary>
        public byte[] SuperColumnName
        {
            set;
            get;
        }

        /// <summary>
        /// get the count retrieved per key after command execution
        /// </summary>
        public Dictionary<byte[], int> Output
        {
            get;
            private set;
        }

        /// <summary>
        /// get or set the predicate to match for the action
        /// </summary>
        public AquilesSlicePredicate Predicate
        {
            get;
            set;
        }

        /// <summary>
        /// Executes a "get_count" over the connection. Return values are set into Output
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            ColumnParent columnParent = this.BuildColumnParent(this.SuperColumnName);
            SlicePredicate slicePredicate = null;
            if (this.Predicate != null)
            {
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(this.Predicate);
            }
            this.Output = cassandraClient.multiget_count(this.Keys, columnParent, slicePredicate, this.GetCassandraConsistencyLevel());
        }
        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();
        }
        
    }
}
