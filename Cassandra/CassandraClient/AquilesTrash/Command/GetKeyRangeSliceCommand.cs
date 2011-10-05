using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;
using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve slices of data on each of the given keys in parallel
    /// </summary>                    
    public class GetKeyRangeSliceCommand : AbstractKeyspaceColumnFamilyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set the Key / Token range to retrieve
        /// </summary>
        public AquilesKeyRange KeyTokenRange
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
        public List<Out> Output
        {
            get;
            private set;
        }

        #region IAquilesCommand Members
        /// <summary>
        /// Executes a "get_range_slices" over the connection. No return values.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            this.Output = null;
            ColumnParent columnParent = this.BuildColumnParent(this.SuperColumn);
            SlicePredicate predicate = ModelConverterHelper.Convert<AquilesSlicePredicate,SlicePredicate>(this.Predicate);
            // Dictionary<string, List<ColumnOrSuperColumn>> output
            KeyRange keyRange = ModelConverterHelper.Convert<AquilesKeyRange, KeyRange>(this.KeyTokenRange);
            List<KeySlice> result = cassandraClient.get_range_slices(columnParent, predicate, keyRange, this.GetCassandraConsistencyLevel());
            this.buildOut(result);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();

            if (this.KeyTokenRange == null)
            {
                throw new AquilesCommandParameterException("A KeyTokenRange must be supplied.");
            }

            this.KeyTokenRange.ValidateForQueryOperation();

            if (this.Predicate == null)
            {
                throw new AquilesCommandParameterException("Predicate cannot be null.");
            }
            else
            {
                this.Predicate.ValidateForQueryOperation();
            }
        }
        #endregion

        private void buildOut(List<KeySlice> output)
        {
            List<GetCommand.Out> columnOrSuperColumnList = null;
            List<Out> returnObjs = new List<Out>();
            Out returnObj = null;
            
            foreach(KeySlice keySlice in output)
            {
                returnObj = new Out();
                returnObj.Key = keySlice.Key;
                columnOrSuperColumnList = new List<GetCommand.Out>(keySlice.Columns.Count);
                foreach (ColumnOrSuperColumn columnOrSuperColumn in keySlice.Columns)
                {
                    columnOrSuperColumnList.Add(new GetCommand.Out()
                    {
                        Column = ModelConverterHelper.Convert<AquilesColumn,Column>(columnOrSuperColumn.Column),
                        SuperColumn = ModelConverterHelper.Convert<AquilesSuperColumn, SuperColumn>(columnOrSuperColumn.Super_column)
                    });
                }
                returnObj.Columns = columnOrSuperColumnList;
                returnObjs.Add(returnObj);
            }

            this.Output = returnObjs;
        }

        /// <summary>
        /// structure to Return Values
        /// </summary>
        public class Out
        {

            /// <summary>
            /// get or set the Key value
            /// </summary>
            public byte[] Key
            {
                get;
                set;
            }

            /// <summary>
            /// get or set the columns (or SuperColumns) within a key 
            /// </summary>
            public List<GetCommand.Out> Columns 
            {
                get;
                set;
            }
        }
    }
}
