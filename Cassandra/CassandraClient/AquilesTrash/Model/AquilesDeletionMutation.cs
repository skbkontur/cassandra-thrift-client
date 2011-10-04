using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Mutation to support elimination
    /// </summary>
    public class AquilesDeletionMutation : IAquilesMutation
    {
        /// <summary>
        /// get or set SuperColumn Name
        /// </summary>
        public byte[] SuperColumn
        {
            get;
            set;
        }

        /// <summary>
        /// get or set SuperColumn Timestamp
        /// </summary>
        public long? Timestamp
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

        #region IAquilesObject<Mutation> Members
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

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter.
        /// Note: Mutations are exclusive for 1 operation, so there is no need to validate for a type of operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void Validate()
        {
            this.ValidateNullPredicateOrSupercolumn();
            this.ValidateTimestamp();
            this.ValidateNullPredicate();
            if (this.Predicate != null)
                this.Predicate.ValidateForDeletationOperation();
        }

        private void ValidateNullPredicateOrSupercolumn()
        {
            if (IsNullSuperColumn() && Predicate == null)
                throw new AquilesCommandParameterException("SuperColumn or Predicate must not be null");
        }

        private bool IsNullSuperColumn()
        {
            return (SuperColumn == null || SuperColumn.Length == 0);
        }

        private void ValidateNullPredicate()
        {
            if (this.Predicate == null)
            {
                throw new AquilesCommandParameterException("A predicate must exist.");
            }
        }

        private void ValidateTimestamp()
        {
            if (this.Timestamp <= 0)
            {
                throw new AquilesCommandParameterException("Timestamp must be greater than 0.");
            }
        }

        private void ValidateNullOrEmptySuperColumn()
        {
            if (this.SuperColumn == null || (this.SuperColumn != null && this.SuperColumn.Length == 0) )
            {
                throw new AquilesCommandParameterException("SuperColumn cannot null or empty.");
            }
        }

        #endregion
    }
}
