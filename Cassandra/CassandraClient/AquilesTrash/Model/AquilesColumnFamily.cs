using System;
using System.Collections.Generic;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Model
{
    /// <summary>
    /// Aquiles structure to hold Keyspace's columnFamily internal information
    /// </summary>
    public class AquilesColumnFamily : IAquilesObject
    {
        //private const int DEFAULTGCGRACESECONDS = 864000;
        //private const double DEFAULTKEYCACHEDSIZE = 0.01;
        //private const double DEFAULTREADREPAIRCHANCE = 1;
        //private const double DEFAULTROWCACHESIZE = 0;
        //private const int DEFAULTMINCOMPACTIONTHRESHOLD = 4;
        //private const int DEFAULTMAXCOMPACTIONTHRESHOLD = 32;
        //private const int DEFAULTROWCACHESAVEPERIODINSECONDS = 0;
        //private const int DEFAULTKEYCACHESAVEPERIODINSECONDS = 3600;
        //private const int DEFAULTMEMTABLEFLUSHAFTERMINS = 60;
        
        /// <summary>
        /// Creates an AquilesColumnFamily with predefined default values
        /// </summary>
        public AquilesColumnFamily() {
            //this.GCGraceSeconds = DEFAULTGCGRACESECONDS;
            //this.KeyCachedSize = DEFAULTKEYCACHEDSIZE;
            //this.ReadRepairChance = DEFAULTREADREPAIRCHANCE;
            //this.RowCacheSize = DEFAULTROWCACHESIZE;
            //this.MinimumCompactationThreshold = DEFAULTMINCOMPACTIONTHRESHOLD;
            //this.MaximumCompactationThreshold = DEFAULTMAXCOMPACTIONTHRESHOLD;
            //this.RowCacheSavePeriodInSeconds = DEFAULTROWCACHESAVEPERIODINSECONDS;
            //this.KeyCacheSavePeriodInSeconds = DEFAULTKEYCACHESAVEPERIODINSECONDS;
            //this.MemtableFlushAfterMins = DEFAULTMEMTABLEFLUSHAFTERMINS;
        }

        /// <summary>
        /// get or set Name
        /// </summary>
        public string Name
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the ColumnFamilyType
        /// </summary>
        public AquilesColumnFamilyType Type
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the comment
        /// </summary>
        public string Comment
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Comparator to be used with Columns
        /// </summary>
        public string Comparator
        {
            get;
            set;
        }
        /// <summary>
        /// get or set the time to wait before garbage collecting tombstones (deletion markers). 
        /// <remarks>defaults to 864000 (10days). See http://wiki.apache.org/cassandra/DistributedDeletes</remarks>
        /// </summary>
        public int? GCGraceSeconds
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the the number of keys per sstable whose locations we keep in memory in "mostly LRU" order.
        /// <remarks>this only cache keys, not columns. Specify a fraction (value less than 1) or an absolute number of keys to cache</remarks>
        /// </summary>
        public double? KeyCachedSize
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the parent Keyspace 
        /// </summary>
        public string Keyspace
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the probability with which read repairs should be invoked on non-quorum reads.  
        /// <remarks>value must be between 0 and 1</remarks>
        /// </summary>
        public double? ReadRepairChance
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the the number of rows whose entire contents we cache in memory. 
        /// <remarks>Do not use this on ColumnFamilies with large rows, or ColumnFamilies with high write:read ratios. Specify a fraction (value less than 1) or an absolute number of rows to cache.</remarks>
        /// </summary>
        public double? RowCacheSize
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Comparator to be used with SubColumns
        /// <remarks>this is used when the columnFamily is set as Super</remarks>
        /// </summary>
        public string SubComparator
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the list of Column Definitions
        /// </summary>
        public List<AquilesColumnDefinition> Columns
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the Id
        /// </summary>
        public int Id
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the minimum tables to merge together
        /// <remarks> The min and max boundaries are the number of tables to attempt to merge together at once. Raising the minimum will make minor compactions take more memory and run less often, lowering the maximum will have the opposite effect. Default for this value is 4.</remarks>
        /// </summary>
        public int? MinimumCompactationThreshold
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the maximum tables to merge together
        /// <remarks> The min and max boundaries are the number of tables to attempt to merge together at once. Raising the minimum will make minor compactions take more memory and run less often, lowering the maximum will have the opposite effect. Default for this value is 32.</remarks>
        /// </summary>
        public int? MaximumCompactationThreshold
        {
            get;
            set;
        }

        /// <summary>
        /// get or set the default validation class
        /// <remarks>Used in conjunction with the validation_class property in the per-column settings to guarantee the type of a column value</remarks>
        /// </summary>
        public string DefaultValidationClass
        {
            get;
            set;
        }

        /// <summary>
        /// TODO
        /// </summary>
        public int? RowCacheSavePeriodInSeconds {
            get;
            set;
        }

        /// <summary>
        /// TODO
        /// </summary>
        public int? KeyCacheSavePeriodInSeconds {
            get;
            set;
        }

        /// <summary>
        /// TODO
        /// </summary>
        public int? MemtableFlushAfterMins {
            get;
            set;
        }

        /// <summary>
        /// TODO
        /// </summary>
        public int? MemtableThroughputInMb {
            get;
            set;
        }

        /// <summary>
        /// TODO
        /// </summary>
        public double? MemtableOperationsInMillions
        {
            get;
            set;
        }

        #region IAquilesObject<CfDef> Members

        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in an insert Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForInsertOperation()
        {
            this.ValidateNotNullOrEmptyKeyspace();

            this.ValidateNotNullOrEmptyName();

            this.ValidateInnerColumns();
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
            //throw new NotImplementedException();
        }
        /// <summary>
        /// Validate the object data to assure consistency when used as input parameter when used in a Query Operation
        /// <remarks>Throw <see cref="Aquiles.Exceptions.AquilesCommandParameterException"/> in case there is something wrong</remarks>
        /// </summary>
        public void ValidateForQueryOperation()
        {
            //throw new NotImplementedException();
        }

        #endregion

        private void ValidateNotNullOrEmptyKeyspace()
        {
            if (String.IsNullOrEmpty(this.Keyspace))
            {
                throw new AquilesCommandParameterException("Keyspace cannot be null or empty.");
            }
        }

        private void ValidateNotNullOrEmptyName()
        {
            if (String.IsNullOrEmpty(this.Name))
            {
                throw new AquilesCommandParameterException("Name cannot be null or empty.");
            }
        }

        private void ValidateInnerColumns()
        {
            if (this.Columns != null)
            {
                foreach (AquilesColumnDefinition columnDefinition in this.Columns)
                {
                    columnDefinition.ValidateForInsertOperation();
                }
            }
        }
    }


}
