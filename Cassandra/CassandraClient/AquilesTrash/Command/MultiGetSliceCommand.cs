using System.Collections.Generic;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Model;
using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve slices of data on each of the given keys in parallel
    /// </summary>                    
    public class MultiGetSliceCommand : AbstractKeyspaceColumnFamilyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set the list of Keys to retrieve
        /// </summary>
        public List<byte[]> Keys
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the name of the SuperColumn
        /// </summary>
        public byte[] SuperColumn
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the predicate to use
        /// </summary>
        public AquilesSlicePredicate Predicate
        {
            get;
            set;
        }

        /// <summary>
        /// get the output of the command
        /// </summary>
        public Out Output
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "multiget_slice" over the connection. No return values.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            this.Output = null;
            ColumnParent columnParent = this.BuildColumnParent(this.SuperColumn);
            SlicePredicate predicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(this.Predicate);
            Dictionary<byte[], List<ColumnOrSuperColumn>> output = cassandraClient.multiget_slice(this.Keys, columnParent, predicate, this.GetCassandraConsistencyLevel());
            this.buildOut(output);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();

            this.ValidateKeys();

            this.ValidatePredicate();
        }

        private void ValidatePredicate()
        {
            if (this.Predicate == null)
            {
                throw new AquilesCommandParameterException("Predicate cannot be null.");
            }
            this.Predicate.ValidateForQueryOperation();
        }


        private void ValidateKeys()
        {
            if ((this.Keys == null) || (this.Keys != null && this.Keys.Count == 0))
            {
                throw new AquilesCommandParameterException("No Keys found.");
            }

            this.ValidateKeyNotNullOrEmpty();
        }

        private void ValidateKeyNotNullOrEmpty()
        {
            foreach (byte[] key in this.Keys)
            {
                if (key == null || key.Length == 0)
                {
                    throw new AquilesCommandParameterException("Key cannot be null or empty.");
                }
            }
        }
        

        private void buildOut(Dictionary<byte[], List<ColumnOrSuperColumn>> output)
        {
            List<GetCommand.Out> columnOrSuperColumnList = null;
            GetCommand.Out columnOrSuperColumn = null;
            Out returnObj = new Out();
            returnObj.Results = new Dictionary<byte[], List<GetCommand.Out>>();
            Dictionary<byte[], List<ColumnOrSuperColumn>>.Enumerator outputEnum = output.GetEnumerator();
            while (outputEnum.MoveNext())
            {
                columnOrSuperColumnList = new List<GetCommand.Out>();
                foreach (ColumnOrSuperColumn cassandraColumnOrSuperColumn in outputEnum.Current.Value)
                {
                    columnOrSuperColumn = new GetCommand.Out();
                    columnOrSuperColumn.Column = ModelConverterHelper.Convert<AquilesColumn, Column>(cassandraColumnOrSuperColumn.Column);
                    columnOrSuperColumn.SuperColumn = ModelConverterHelper.Convert<AquilesSuperColumn, SuperColumn>(cassandraColumnOrSuperColumn.Super_column);
                    columnOrSuperColumnList.Add(columnOrSuperColumn);
                }
                returnObj.Results.Add(outputEnum.Current.Key, columnOrSuperColumnList);
            }

            this.Output = returnObj;
        }

        /// <summary>
        /// structure to Return Values
        /// </summary>
        public class Out
        {

            /// <summary>
            /// get or set the results 
            /// <remarks>the dictionary key is actually the key used over cassandra</remarks>
            /// </summary>
            public Dictionary<byte[], List<GetCommand.Out>> Results 
            {
                get;
                set;
            }

        }
    }
}
