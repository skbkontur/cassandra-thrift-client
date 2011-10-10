using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra Key Range structure (only support for a key range get)
    /// <remarks>StartKey will be included in the response</remarks>
    /// </summary>
    public class AquilesKeyRange : IAquilesObject
    {
        /// <summary>
        /// get or set the Start Key (included in the command response)
        /// </summary>
        public byte[] StartKey
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Endkey
        /// </summary>
        public byte[] EndKey
        {
            get;
            set;
        }

        /// <summary>
        /// get or set how many keys to permit in the KeyRange
        /// </summary>
        public int Count
        {
            get;
            set;
        }
        
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
            this.ValidateCountGreaterThanZero();
            this.ValidateNullOrEmptyStartKey();
            this.ValidateNullOrEmptyEndKey();
        }

        private void ValidateNullOrEmptyEndKey()
        {
            if (this.EndKey == null)
            {
                throw new AquilesCommandParameterException("EndKey must not be null.");
            }
        }

        private void ValidateNullOrEmptyStartKey()
        {
            if (this.StartKey == null)
            {
                throw new AquilesCommandParameterException("StartKey must not be null.");
            }
        }

        private void ValidateCountGreaterThanZero()
        {
            if (this.Count <= 0)
            {
                throw new AquilesCommandParameterException("Quantity of keys is required.");
            }
        }

        
    }
}
