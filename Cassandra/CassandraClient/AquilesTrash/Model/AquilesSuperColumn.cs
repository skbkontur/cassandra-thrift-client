using System;
using System.Collections.Generic;

using System.Globalization;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra SuperColumn structure
    /// </summary>
    public class AquilesSuperColumn : IAquilesObject
    {
        /// <summary>
        /// get or set a List of columns
        /// </summary>
        public List<AquilesColumn> Columns 
        {
            get; 
            set; 
        }

        /// <summary>
        /// get or set the name
        /// </summary>
        public byte[] Name
        {
            get;
            set;
        }

        /// <summary>
        /// ctor
        /// </summary>
        public AquilesSuperColumn() 
        {
            this.Columns = new List<AquilesColumn>();
        }

        #region IAquilesObject<SuperColumn> Members
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an insert Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForInsertOperation()
        {
            this.Validate();

            foreach (AquilesColumn column in this.Columns)
            {
                column.ValidateForInsertOperation();
            }
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            this.Validate();

            foreach (AquilesColumn column in this.Columns)
            {
                column.ValidateForDeletationOperation();
            }
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            this.Validate();

            foreach (AquilesColumn column in this.Columns)
            {
                column.ValidateForSetOperation();
            }
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            this.Validate();

            foreach (AquilesColumn column in this.Columns)
            {
                column.ValidateForQueryOperation();
            }
        }

        private void Validate()
        {
            this.ValidateNorNullnorEmptyName();
            this.ValidateNoColumns();
        }

        private void ValidateNoColumns()
        {
            if (this.Columns == null)
            {
                throw new AquilesCommandParameterException("SuperColumn must have at least 1 child columns.");
            }
        }

        private void ValidateNorNullnorEmptyName()
        {
            if (this.Name == null || (this.Name != null && this.Name.Length == 0))
            {
                throw new AquilesCommandParameterException("Name must be not null");
            }
        }

        #endregion

        /// <summary>
        /// overriding ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "Name: '{0}', Columns' size: '{1}'", this.Name, this.Columns.Count);
        }
    }
}
