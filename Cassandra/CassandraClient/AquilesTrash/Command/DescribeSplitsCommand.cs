using System;
using System.Collections.Generic;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to describe key splits from a given ColumnFamily.
    /// <remarks>experimental API for hadoop/parallel query support.  May change violently and without warning.</remarks>
    /// </summary>
    public class DescribeSplitsCommand : AbstractKeyspaceColumnFamilyDependantCommand, IAquilesCommand
    {

        /// <summary>
        /// get or set the start token 
        /// </summary>
        public string StartToken
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the end token 
        /// </summary>
        public string EndToken
        {
            get;
            set;
        }

        /// <summary>
        /// get or set key quantity per split.
        /// </summary>
        public int KeysPerSplit
        {
            get;
            set;
        }

        /// <summary>
        /// returns list of token strings such that first subrange is (list[0], list[1]], next is (list[1], list[2]], etc.
        /// </summary>
        public List<string> Output
        {
            get;
            private set;
        }

        /// <summary>
        /// Executes a "describe_splits" over the connection. Returns list of token strings such that first subrange is (list[0], list[1]], next is (list[1], list[2]], etc.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            this.Output = cassandraClient.describe_splits(this.ColumnFamily, this.StartToken, this.EndToken, this.KeysPerSplit);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();
            if (String.IsNullOrEmpty(this.StartToken))
            {
                throw new AquilesCommandParameterException("StartToken must be not null or empty.");
            }
            if (String.IsNullOrEmpty(this.EndToken))
            {
                throw new AquilesCommandParameterException("EndToken must be not null or empty.");
            }
            if (this.KeysPerSplit <= 0)
            {
                throw new AquilesCommandParameterException("KeyPerSplit number must be greater than 0.");
            }
        }

        
    }
}
