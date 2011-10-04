using System;
using Apache.Cassandra;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Abstract class for an AquilesCommand that needs to have a Keyspace and a ColumnFamily
    /// </summary>
    public abstract class AbstractKeyspaceColumnFamilyDependantCommand : AbstractKeyspaceDependantCommand
    {
        /// <summary>
        /// get or set the columnFamily
        /// </summary>
        public string ColumnFamily
        {
            set;
            get;
        }

        /// <summary>
        /// Build Cassandra Thrift ColumnPath
        /// </summary>
        /// <param name="supercolumn">supercolumn name</param>
        /// <param name="column">column name</param>
        /// <returns>Cassandra Thrift ColumnPath</returns>
        protected ColumnPath BuildColumnPath(byte[] supercolumn, byte[] column)
        {
            return this.BuildColumnPath(this.ColumnFamily, supercolumn, column);
        }

        /// <summary>
        /// Build Cassandra Thrift ColumnPath
        /// </summary>
        /// <param name="columnFamily">columnfamily</param>
        /// <param name="supercolumn">supercolumn name</param>
        /// <param name="column">column name</param>
        /// <returns>Cassandra Thrift ColumnPath</returns>
        protected ColumnPath BuildColumnPath(string columnFamily, byte[] supercolumn, byte[] column)
        {
            ColumnPath columnPath = null;
            bool isSuperColumnMissing = supercolumn == null;
            bool isColumnMissing = column == null;

            columnPath = new ColumnPath();
            columnPath.Column_family = columnFamily;
            if (!isSuperColumnMissing)
            {
                columnPath.Super_column = supercolumn;
            }
            if (!isColumnMissing)
            {
                columnPath.Column = column;
            }

            return columnPath;
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public virtual void ValidateInput()
        {
            this.ValidateColumnFamilyNotNullOrEmpty();
        }

        private void ValidateColumnFamilyNotNullOrEmpty()
        {
            if (String.IsNullOrEmpty(this.ColumnFamily))
            {
                throw new AquilesCommandParameterException("ColumnFamily must be not null or empty.");
            }
        }

        /// <summary>
        /// Biuld Thrift ColumnParent structure using ColumnFamily and SuperColumn information
        /// </summary>
        /// <param name="superColumn">name for the supercolumn (null in case there is not one)</param>
        /// <returns>Thrift ColumnParent</returns>
        protected ColumnParent BuildColumnParent(byte[] superColumn)
        {
            ColumnParent columnParent = new ColumnParent();

            bool isSuperColumnMissing = superColumn == null;

            columnParent.Column_family = this.ColumnFamily;
            if (!isSuperColumnMissing)
            {
                columnParent.Super_column = superColumn;
            }
            return columnParent;
        }
    }
}
