using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;
using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve a Column or SuperColumn from Keyspace of a given cluster with the given key
    /// </summary>
    public class GetCountCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set the SuperColumn
        /// </summary>
        public byte[] SuperColumnName
        {
            set;
            get;
        }

        /// <summary>
        /// get the count retrieved after command execution
        /// </summary>
        public int Count
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
                slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate,SlicePredicate>(this.Predicate);
            }
            this.Count = cassandraClient.get_count(this.Key, columnParent, slicePredicate, this.GetCassandraConsistencyLevel());
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
