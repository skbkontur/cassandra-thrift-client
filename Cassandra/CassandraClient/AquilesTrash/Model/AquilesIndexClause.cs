using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra IndexClause structure
    /// </summary>
    public class AquilesIndexClause : IAquilesObject
    {
        /// <summary>
        /// Get or set the list of IndexExpressions to AND together. 
        /// </summary>
        public List<AquilesIndexExpression> Expressions
        {
            get;
            set;
        }

        /// <summary>
        /// Get or set the start key range to begin searching on
        /// </summary>
        public byte[] StartKey
        {
            get;
            set;
        }

        /// <summary>
        /// Get or set the maximum rows to return
        /// <remarks>Default value is 100</remarks>
        /// </summary>
        public int? Count
        {
            set;
            get;
        }

        public void ValidateForInsertOperation()
        {
            // DO NOTHING
        }

        public void ValidateForDeletationOperation()
        {
            // DO NOTHING
        }

        public void ValidateForSetOperation()
        {
            // DO NOTHING
        }

        public void ValidateForQueryOperation()
        {
            if (this.Expressions == null)
            {
                throw new AquilesCommandParameterException("Expressions cannot be null.");
            }

            if (this.StartKey == null)
            {
                throw new AquilesCommandParameterException("StartKey cannot be null.");
            }

            foreach (AquilesIndexExpression indexExpression in this.Expressions)
            {
                indexExpression.ValidateForQueryOperation();
            }


        }

        
    }
}
