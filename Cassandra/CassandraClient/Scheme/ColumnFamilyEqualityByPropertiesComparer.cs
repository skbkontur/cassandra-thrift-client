using System;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    internal class ColumnFamilyEqualityByPropertiesComparer
    {
        public bool NeedUpdateColumnFamily(ColumnFamily columnFamilyWithNewProperties, ColumnFamily columnFamilyFromTarget)
        {
            if(columnFamilyWithNewProperties.Name != columnFamilyFromTarget.Name)
                throw new InvalidOperationException(string.Format("Cannot compare ColumnFamilies with different names ('{0}' and '{1}')", columnFamilyWithNewProperties.Name, columnFamilyFromTarget.Name));
            if(!CompareComparatorType(columnFamilyWithNewProperties.ComparatorType, columnFamilyFromTarget.ComparatorType))
                throw new InvalidOperationException(string.Format("Cannot compare ColumnFamilies with different comparatorTypes ('{0}' and '{1}')", columnFamilyWithNewProperties.ComparatorType, columnFamilyFromTarget.ComparatorType));
            return
                !(
                     columnFamilyWithNewProperties.Caching.Equals(columnFamilyFromTarget.Caching) &&
                     (columnFamilyWithNewProperties.ReadRepairChance == null || columnFamilyWithNewProperties.ReadRepairChance.Equals(columnFamilyFromTarget.ReadRepairChance)) &&
                     (columnFamilyWithNewProperties.GCGraceSeconds == null || columnFamilyWithNewProperties.GCGraceSeconds.Equals(columnFamilyFromTarget.GCGraceSeconds)) &&
                     (columnFamilyWithNewProperties.CompactionStrategy == null || CompareCompactionStrategy(columnFamilyWithNewProperties.CompactionStrategy, columnFamilyFromTarget.CompactionStrategy)) &&
                     (columnFamilyWithNewProperties.Compression == null || CompareCompression(columnFamilyWithNewProperties.Compression, columnFamilyFromTarget.Compression)) &&
                     (columnFamilyWithNewProperties.BloomFilterFpChance == null || columnFamilyWithNewProperties.BloomFilterFpChance.Equals(columnFamilyFromTarget.BloomFilterFpChance)) &&
                     (columnFamilyWithNewProperties.DefaultTtl == null || columnFamilyWithNewProperties.DefaultTtl.Equals(columnFamilyFromTarget.DefaultTtl))
                 );
        }

        private static bool CompareComparatorType(ColumnComparatorType left, ColumnComparatorType right)
        {
            return left != null && right != null && left.IsComposite.Equals(right.IsComposite) && left.Types.SequenceEqual(right.Types);
        }

        private static bool CompareCompression(ColumnFamilyCompression lhs, ColumnFamilyCompression rhs)
        {
            if(lhs == null && rhs == null)
                return true;
            if(lhs != null && rhs != null)
                return lhs.Algorithm == rhs.Algorithm && CompareCompressionOptions(lhs.Options, rhs.Options);
            return CompareCompression(lhs ?? ColumnFamilyCompression.Default, rhs ?? ColumnFamilyCompression.Default);
        }

        private static bool CompareCompressionOptions(CompressionOptions lhs, CompressionOptions rhs)
        {
            if(lhs == null && rhs == null)
                return true;
            if(lhs != null && rhs != null)
                return lhs.ChunkLengthInKb == rhs.ChunkLengthInKb && lhs.CrcCheckChance == rhs.CrcCheckChance;
            return false;
        }

        private static bool CompareCompactionStrategy(CompactionStrategy lhs, CompactionStrategy rhs)
        {
            if(lhs == null && rhs == null)
                return true;
            if(lhs != null && rhs != null)
                return lhs.CompactionStrategyType == rhs.CompactionStrategyType && CompareCompactionStrategyOptions(lhs.CompactionStrategyOptions, rhs.CompactionStrategyOptions);
            return false;
        }

        private static bool CompareCompactionStrategyOptions(CompactionStrategyOptions lhs, CompactionStrategyOptions rhs)
        {
            if(lhs == null && rhs == null)
                return true;
            if(lhs != null && rhs != null)
            {
                return lhs.Enabled == rhs.Enabled &&
                       lhs.MinThreshold == rhs.MinThreshold &&
                       lhs.MaxThreshold == rhs.MaxThreshold &&
                       lhs.SstableSizeInMb == rhs.SstableSizeInMb;
            }
            return false;
        }
    }
}