using System;
using System.Globalization;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to contains Cassandra Column structure
    /// </summary>
    public class AquilesColumn : IAquilesObject
    {
        /// <summary>
        /// Cassandra column name
        /// </summary>
        public byte[] ColumnName
        {
            get;
            set;
        }

        /// <summary>
        /// Cassandra column value
        /// </summary>
        public byte[] Value
        {
            get;
            set;
        }

        /// <summary>
        /// Cassandra Column TimeStamp (must match unix timestamp)
        /// <remarks>Don't mess with this unless you know what you are doing</remarks>
        /// </summary>
        public long? Timestamp
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Time To Live
        /// <remarks>If this value is null (not set) then the column will not be evicted and needs to be removed by a DeleteCommand</remarks>
        /// </summary>
        public int? TTL
        {
            get;
            set;
        }

        /// <summary>
        /// Empty Constructor
        /// </summary>
        public AquilesColumn()
        {
        }

        #region IAquilesObject<Column> Members

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an insert Operation
        /// <remarks>Throw <see cref="CassandraClient.AquilesTrash.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForInsertOperation()
        {
            this.ValidateNullOrEmptyColumnName();

            this.ValidateNullValue();

            this.ValidateTTL();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an deletation Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForDeletationOperation()
        {
            this.ValidateNullOrEmptyColumnName();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an set / update Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForSetOperation()
        {
            this.ValidateNullOrEmptyColumnName();
        }

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in a Query Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            this.ValidateNullOrEmptyColumnName();
        }

        private void ValidateNullValue()
        {
            if (this.Value == null)
            {
                throw new AquilesCommandParameterException("Value cannot be null.");
            }
        }

        private void ValidateTTL()
        {
            if (this.TTL.HasValue && (this.TTL <= 0))
            {
                throw new AquilesCommandParameterException("TTL must be greater than 0.");
            }
        }
        private void ValidateNullOrEmptyColumnName()
        {
            if ((this.ColumnName == null) || (this.ColumnName != null && this.ColumnName.Length == 0 ) )
            {
                throw new AquilesCommandParameterException("ColumnName cannot be null or empty.");
            }
        }
        #endregion

        /// <summary>
        /// overriding ToString
        /// </summary>
        /// <returns></returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "Name: '{0}', Timestamp: '{1}', Value: '{2}', TTL: '{3}'",
                this.ColumnName,
                this.Timestamp,
                this.Value,
                this.TTL);
        }
    }
}
