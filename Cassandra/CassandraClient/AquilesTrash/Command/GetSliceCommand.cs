using System.Collections.Generic;

using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to retrieve slices of data on the given key
    /// </summary>
    public class GetSliceCommand : AbstractKeyspaceColumnFamilyKeyDependantCommand, IAquilesCommand
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

        #region IAquilesCommand Members
        /// <summary>
        /// Executes a "get_slice" over the connection. No return values.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            this.Output = null;
            ColumnParent columnParent = this.BuildColumnParent(this.SuperColumn);
            SlicePredicate predicate = ModelConverterHelper.Convert<AquilesSlicePredicate,SlicePredicate>(this.Predicate);
            List<ColumnOrSuperColumn> output = cassandraClient.get_slice(this.Key, columnParent, predicate, this.GetCassandraConsistencyLevel());
            this.buildOut(output);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();

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

        private void buildOut(List<ColumnOrSuperColumn> output)
        {
            this.Output = new Out();
            this.Output.Results = new List<GetCommand.Out>();
            foreach (ColumnOrSuperColumn columnOrSuperColumn in output)
            {
                this.Output.Results.Add(new GetCommand.Out()
                    {
                        Column = ModelConverterHelper.Convert<AquilesColumn, Column>(columnOrSuperColumn.Column),
                        SuperColumn = ModelConverterHelper.Convert<AquilesSuperColumn, SuperColumn>(columnOrSuperColumn.Super_column)
                    });
            }
        }

        /// <summary>
        /// structure to Return Values
        /// </summary>
        public class Out
        {

            /// <summary>
            /// get or set the results 
            /// </summary>
            public List<GetCommand.Out> Results
            {
                get;
                set;
            }

        }

    }
}
