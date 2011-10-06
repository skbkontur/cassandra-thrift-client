using System.Collections.Generic;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    ///  Aquiles structure to contains Cassandra Slice Predicate structure
    /// </summary>
    public class AquilesSlicePredicate : IAquilesObject
    {
        /// <summary>
        /// get or set the List of Column Names
        /// <remarks>Columns anr SliceRange are are mutually exclusive</remarks>
        /// </summary>
        public List<byte[]> Columns
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the SliceRange
        /// <remarks>Columns anr SliceRange are are mutually exclusive</remarks>
        /// </summary>
        public AquilesSliceRange SliceRange
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
            if (this.SliceRange != null)
            {
                this.SliceRange.ValidateForInsertOperation();
            }
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            this.Validate();
            if (this.SliceRange != null)
            {
                this.SliceRange.ValidateForDeletationOperation();
            }
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            this.Validate();
            if (this.SliceRange != null)
            {
                this.SliceRange.ValidateForSetOperation();
            }
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            this.Validate();
            if (this.SliceRange != null)
            {
                this.SliceRange.ValidateForQueryOperation();
            }
        }

        private void Validate()
        {
            this.ValidateMutualExclusion();

            this.ValidateNoColumnsNorSliceRange();

            if (this.Columns != null) 
            {
                this.ValidateColumns();
            }
        }

        private void ValidateNoColumnsNorSliceRange()
        {
            if ((this.Columns == null) && (this.SliceRange == null))
            {
                throw new AquilesCommandParameterException("Columns or SliceRange information must be present.");
            }
        }

        private void ValidateMutualExclusion()
        {
            if ((this.Columns != null) && (this.SliceRange != null))
            {
                throw new AquilesCommandParameterException("Columns and SliceRange are mutually exclusive.");
            }
        }

        private void ValidateColumns()
        {
            this.ValidateColumnQuantity();
            foreach (byte[] column in this.Columns)
            {
                ValidateColumnNotNullOrEmpty(column);
            }
        }

        private static void ValidateColumnNotNullOrEmpty(byte[] column)
        {
            if ( (column == null) || (column != null && column.Length == 0) )
            {
                throw new AquilesCommandParameterException("Empty ColumnName is not supported.");
            }
        }

        private void ValidateColumnQuantity()
        {
            if (this.Columns.Count == 0)
            {
                throw new AquilesCommandParameterException("No columns especified.");
            }
        }

        
    }
}
