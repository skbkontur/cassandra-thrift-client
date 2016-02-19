using System;
using System.Collections.Generic;
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
                     (columnFamilyWithNewProperties.Caching.Equals(columnFamilyFromTarget.Caching)) &&
                     (columnFamilyWithNewProperties.ReadRepairChance == null || columnFamilyWithNewProperties.ReadRepairChance.Equals(columnFamilyFromTarget.ReadRepairChance)) &&
                     (columnFamilyWithNewProperties.GCGraceSeconds == null || columnFamilyWithNewProperties.GCGraceSeconds.Equals(columnFamilyFromTarget.GCGraceSeconds)) &&
                     (columnFamilyWithNewProperties.Indexes == null || CompareIndexes(columnFamilyWithNewProperties.Indexes, columnFamilyFromTarget.Indexes)) &&
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

        private static bool CompareCompression(ColumnFamilyCompression leftCompression, ColumnFamilyCompression rightCompression)
        {
            if(leftCompression == null && rightCompression == null)
                return true;
            if((leftCompression != null && rightCompression != null))
            {
                return
                    (
                        leftCompression.Algorithm == rightCompression.Algorithm &&
                        (
                            (leftCompression.Options == null && rightCompression.Options == null) ||
                            (
                                (leftCompression.Options != null && rightCompression.Options != null) &&
                                leftCompression.Options.ChunkLengthInKb == rightCompression.Options.ChunkLengthInKb &&
                                leftCompression.Options.CrcCheckChance == rightCompression.Options.CrcCheckChance
                            )
                        )
                    );
            }
            return CompareCompression(leftCompression ?? ColumnFamilyCompression.Default, rightCompression ?? ColumnFamilyCompression.Default);
        }

        private static bool CompareCompactionStrategy(CompactionStrategy compactionStrategy, CompactionStrategy compactionStrategy1)
        {
            if((compactionStrategy == null && compactionStrategy1 == null))
                return true;
            if((compactionStrategy != null && compactionStrategy1 != null))
            {
                return
                    (
                        compactionStrategy.CompactionStrategyType == compactionStrategy1.CompactionStrategyType &&
                        ((compactionStrategy.CompactionStrategyOptions == null && compactionStrategy1.CompactionStrategyOptions == null) ||
                         (compactionStrategy.CompactionStrategyOptions != null && compactionStrategy1.CompactionStrategyOptions != null &&
                          compactionStrategy.CompactionStrategyOptions.SstableSizeInMb == compactionStrategy1.CompactionStrategyOptions.SstableSizeInMb)) &&
                        (compactionStrategy.MinCompactionThreshold == null || compactionStrategy.MinCompactionThreshold == compactionStrategy1.MinCompactionThreshold) &&
                        (compactionStrategy.MaxCompactionThreshold == null || compactionStrategy.MaxCompactionThreshold == compactionStrategy1.MaxCompactionThreshold)
                    );
            }
            return false;
        }

        private static bool CompareIndexes(IEnumerable<IndexDefinition> leftIndices, IEnumerable<IndexDefinition> rightIndices)
        {
            if(leftIndices == null && rightIndices == null)
                return true;
            if(leftIndices != null && rightIndices == null)
                return false;
            if(leftIndices == null)
                return false;

            var sortedLeftIndices = leftIndices.OrderBy(x => x.Name).ToArray();
            var sortedRightIndices = rightIndices.OrderBy(x => x.Name).ToArray();
            if(sortedLeftIndices.Length != sortedRightIndices.Length)
                return false;
            return sortedLeftIndices
                .Zip(
                    sortedRightIndices,
                    (x, y) => x.Name == y.Name && x.ValidationClass == y.ValidationClass)
                .All(x => x);
        }
    }
}