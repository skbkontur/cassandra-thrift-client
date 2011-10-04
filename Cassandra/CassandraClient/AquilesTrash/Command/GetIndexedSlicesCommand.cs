
using System.Collections.Generic;
using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;


using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to returns a list of key slices that meets the IndexClause critera. 
    /// <remarks>
    /// Note that index clause must contain at least a single EQ operation. 
    /// The columns specified in the IndexExpressions will also need to be specified as indexed when the CF is created.
    /// </remarks>
    /// </summary>
    public class GetIndexedSlicesCommand : AbstractKeyspaceColumnFamilyDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set the name of the SuperColumn
        /// </summary>
        public byte[] SuperColumn
        {
            get;
            set;
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
        /// get or set the index clause (search criteria)
        /// </summary>
        public AquilesIndexClause IndexClause
        {
            get;
            set;
        }

        /// <summary>
        /// get the output of the command
        /// </summary>
        public List<Out> Output
        {
            private set;
            get;
        }

        #region IAquilesCommand Members
        /// <summary>
        /// Executes a "get_indexed_slices" over the connection.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            ColumnParent columnParent = this.BuildColumnParent(this.SuperColumn);
            IndexClause indexClause = ModelConverterHelper.Convert<AquilesIndexClause, IndexClause>(this.IndexClause);
            SlicePredicate slicePredicate = ModelConverterHelper.Convert<AquilesSlicePredicate, SlicePredicate>(this.Predicate);
            List<KeySlice> result = cassandraClient.get_indexed_slices(columnParent, indexClause, slicePredicate, this.GetCassandraConsistencyLevel());
            this.BuildOutput(result);
        }
        #endregion

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();

            this.IndexClause.ValidateForQueryOperation();

            this.Predicate.ValidateForQueryOperation();
        }

        private void BuildOutput(List<KeySlice> result)
        {
           List<GetCommand.Out> columnOrSuperColumnList = null;
            List<Out> returnObjs = new List<Out>();
            Out returnObj = null;
            
            foreach(KeySlice keySlice in result)
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
