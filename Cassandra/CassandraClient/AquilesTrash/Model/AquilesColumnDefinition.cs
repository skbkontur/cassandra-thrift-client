using System;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra Column structure definition (for schema administration)
    /// </summary>
    public class AquilesColumnDefinition : IAquilesObject
    {
        /// <summary>
        /// get or set the Name
        /// </summary>
        public byte[] Name
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Index Name
        /// </summary>
        public string IndexName
        {
            get;
            set;
        }

        /// <summary>
        /// get or set if this Column is part of an index
        /// </summary>
        public bool IsIndex
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Validation Class
        /// </summary>
        public string ValidationClass
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
            this.ValidateNotNullorEmptyName();
            this.ValidateNotNullOrEmptyValidationClass();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            //throw new NotImplementedException();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            this.ValidateNotNullorEmptyName();
            this.ValidateNotNullOrEmptyValidationClass();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in a Query Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            //throw new NotImplementedException();
        }

        

        private void ValidateNotNullOrEmptyValidationClass()
        {
            if (String.IsNullOrEmpty(this.ValidationClass))
            {
                throw new AquilesCommandParameterException("Validation Class cannot be null or empty");
            }
        }

        private void ValidateNotNullorEmptyName()
        {
            if (this.Name == null || this.Name.Length == 0)
            {
                throw new AquilesCommandParameterException("Column Name cannot be null or empty");
            }
        }
    }
}
