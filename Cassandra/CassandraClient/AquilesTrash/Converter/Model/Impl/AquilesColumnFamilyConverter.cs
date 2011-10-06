using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using CassandraClient.AquilesTrash.Model;
using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Converter.Model;

namespace CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesColumnFamily
    /// </summary>
    public class AquilesColumnFamilyConverter : IThriftConverter<AquilesColumnFamily, CfDef>
    {
        /// <summary>
        /// Transform AquilesColumnFamily structure into CfDef
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public CfDef Transform(AquilesColumnFamily objectA)
        {
            CfDef columnFamily = new CfDef();
            // REQUIRED
            columnFamily.Keyspace = objectA.Keyspace;
            columnFamily.Name = objectA.Name;

            // OPTIONAL
            columnFamily.Column_type = objectA.Type.ToString();
            columnFamily.Comment = objectA.Comment;
            if (!String.IsNullOrEmpty(objectA.Comparator))
            {
                columnFamily.Comparator_type = objectA.Comparator;
            }
            if (objectA.GCGraceSeconds.HasValue)
            {
                columnFamily.Gc_grace_seconds = objectA.GCGraceSeconds.Value;
            }
            if (objectA.KeyCachedSize.HasValue)
            {
                columnFamily.Key_cache_size = objectA.KeyCachedSize.Value;
            }
            if (objectA.ReadRepairChance.HasValue)
            {
                columnFamily.Read_repair_chance = objectA.ReadRepairChance.Value;
            }
            if (objectA.RowCacheSize.HasValue)
            {
                columnFamily.Row_cache_size = objectA.RowCacheSize.Value;
            }
            columnFamily.Subcomparator_type = objectA.SubComparator;
            columnFamily.Default_validation_class = objectA.DefaultValidationClass;
            columnFamily.Id = objectA.Id;
            if (objectA.MinimumCompactationThreshold.HasValue)
            {
                columnFamily.Min_compaction_threshold = objectA.MinimumCompactationThreshold.Value;
            }
            if (objectA.MaximumCompactationThreshold.HasValue)
            {
                columnFamily.Max_compaction_threshold = objectA.MaximumCompactationThreshold.Value;
            }
            if (objectA.RowCacheSavePeriodInSeconds.HasValue)
            {
                columnFamily.Row_cache_save_period_in_seconds = objectA.RowCacheSavePeriodInSeconds.Value;
            }
            if (objectA.KeyCacheSavePeriodInSeconds.HasValue)
            {
                columnFamily.Key_cache_save_period_in_seconds = objectA.KeyCacheSavePeriodInSeconds.Value;
            }
            if (objectA.MemtableFlushAfterMins.HasValue)
            {
                columnFamily.Memtable_flush_after_mins = objectA.MemtableFlushAfterMins.Value;
            }
            if (objectA.MemtableThroughputInMb.HasValue)
            {
                columnFamily.Memtable_throughput_in_mb = objectA.MemtableThroughputInMb.Value;
            }
            if (objectA.MemtableOperationsInMillions.HasValue)
            {
                columnFamily.Memtable_operations_in_millions = objectA.MemtableOperationsInMillions.Value;
            }

            if (objectA.Columns != null)
            {
                List<AquilesColumnDefinition> originColumns = objectA.Columns;
                List<ColumnDef> destinationColumns = new List<ColumnDef>(originColumns.Count);
                foreach (AquilesColumnDefinition originColumn in originColumns)
                {
                    destinationColumns.Add(ModelConverterHelper.Convert<AquilesColumnDefinition,ColumnDef>(originColumn));
                }
                columnFamily.Column_metadata = destinationColumns;
            }

            return columnFamily;
        }

        /// <summary>
        /// Transform CfDef structure into AquilesColumnFamily
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesColumnFamily Transform(CfDef objectB)
        {
            AquilesColumnFamily columnFamily = new AquilesColumnFamily();
            columnFamily.Name = objectB.Name;
            columnFamily.Type = (AquilesColumnFamilyType) Enum.Parse(typeof(AquilesColumnFamilyType), objectB.Column_type);
            columnFamily.Comment = objectB.Comment;
            columnFamily.Comparator = objectB.Comparator_type;
            columnFamily.GCGraceSeconds = objectB.Gc_grace_seconds;
            columnFamily.KeyCachedSize = objectB.Key_cache_size;
            columnFamily.Keyspace = objectB.Keyspace;
            columnFamily.ReadRepairChance = objectB.Read_repair_chance;
            columnFamily.RowCacheSize = objectB.Row_cache_size;
            columnFamily.SubComparator = objectB.Subcomparator_type;
            columnFamily.DefaultValidationClass = objectB.Default_validation_class;
            columnFamily.Id = objectB.Id;
            columnFamily.MinimumCompactationThreshold = objectB.Min_compaction_threshold;
            columnFamily.MaximumCompactationThreshold = objectB.Max_compaction_threshold;
            columnFamily.RowCacheSavePeriodInSeconds = objectB.Row_cache_save_period_in_seconds;
            columnFamily.KeyCacheSavePeriodInSeconds = objectB.Key_cache_save_period_in_seconds;
            columnFamily.MemtableFlushAfterMins = objectB.Memtable_flush_after_mins;
            columnFamily.MemtableThroughputInMb = objectB.Memtable_throughput_in_mb;
            columnFamily.MemtableOperationsInMillions = objectB.Memtable_operations_in_millions;
            if (objectB.Column_metadata != null)
            {
                List<ColumnDef> originColumns = objectB.Column_metadata;
                List<AquilesColumnDefinition> destinationColumns = new List<AquilesColumnDefinition>(originColumns.Count);
                foreach (ColumnDef originColumn in originColumns)
                {
                    destinationColumns.Add(ModelConverterHelper.Convert<AquilesColumnDefinition,ColumnDef>(originColumn));
                }
                columnFamily.Columns = destinationColumns;
            }

            return columnFamily;
        }

        
    }
}
