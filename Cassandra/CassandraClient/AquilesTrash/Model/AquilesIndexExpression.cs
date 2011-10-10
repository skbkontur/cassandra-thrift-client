using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra IndexExpression structure
    /// </summary>
    public class AquilesIndexExpression : IAquilesObject
    {
        /// <summary>
        /// Get or set the ColumnName to perform the operand on
        /// </summary>
        public byte[] ColumnName
        {
            get;
            set;
        }

        /// <summary>
        /// Get or set the IndexOperator to apply
        /// </summary>
        public AquilesIndexOperator IndexOperator
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Value to use in the comparison
        /// </summary>
        public byte[] Value
        {
            get;
            set;
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
            if (this.ColumnName == null || this.ColumnName.Length == 0)
            {
                throw new AquilesCommandParameterException("ColumnName cannot be null or empty.");
            }

            if (this.Value == null)
            {
                throw new AquilesCommandParameterException("Value cannot be null.");
            }
        }

        
    }
}
