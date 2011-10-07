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

        /// <summary>
        /// Executes a "get_range_slices" over the connection. No return values.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public override void Execute(Cassandra.Client cassandraClient)
        {
            this.Output = null;
            ColumnParent columnParent = this.BuildColumnParent();
            SlicePredicate predicate = ModelConverterHelper.Convert<AquilesSlicePredicate,SlicePredicate>(this.Predicate);
            // Dictionary<string, List<ColumnOrSuperColumn>> output
            KeyRange keyRange = ModelConverterHelper.Convert<AquilesKeyRange, KeyRange>(this.KeyTokenRange);
            List<KeySlice> result = cassandraClient.get_range_slices(columnParent, predicate, keyRange, this.GetCassandraConsistencyLevel());
            this.BuildOut(result);
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
        

        private void BuildOut(IEnumerable<KeySlice> output)
        {
            var returnObjs = new List<Out>();

            foreach(KeySlice keySlice in output)
            {
                var returnObj = new Out {Key = keySlice.Key};
                var columnOrSuperColumnList = new List<AquilesColumn>(keySlice.Columns.Count);
                columnOrSuperColumnList.AddRange(keySlice.Columns.Select(columnOrSuperColumn => ModelConverterHelper.Convert<AquilesColumn, Column>(columnOrSuperColumn.Column)));
                returnObj.Columns = columnOrSuperColumnList;
                returnObjs.Add(returnObj);
            }

            this.Output = returnObjs;
        }

        public class Out
        {
            public byte[] Key
            {
                get;
                set;
            }
            public List<AquilesColumn> Columns 
            {
                get;
                set;
            }
        }
    }
}
