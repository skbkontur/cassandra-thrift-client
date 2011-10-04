using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra Slice Range structure
    /// </summary>
    public class AquilesSliceRange : IAquilesObject
    {
        /// <summary>
        /// get or set how many columns to return.
        /// </summary>
        public int Count
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the column name to start the slice with
        /// </summary>
        public byte[] StartColumn
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the column name to finish the slice with
        /// </summary>
        public byte[] FinishColumn
        {
            get;
            set;
        }

        /// <summary>
        /// get or set if the order of the result should be reversed
        /// </summary>
        public bool Reversed
        {
            get;
            set;
        }

        #region IAquilesObject<SliceRange> Members
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an insert Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForInsertOperation()
        {
            this.Validate();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            this.Validate();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            this.Validate();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            this.Validate();
        }

        private void Validate()
        {
            if (this.Count <= 0)
            {
                throw new AquilesCommandParameterException("Count must be greater than 0.");
            }
        }

        #endregion
    }
}
