using System;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace Cassandra.ThriftClient.Tests.UnitTests.CoreTests
{
    [TestFixture]
    public class ColumnFamilyEqualityByPropertiesComparerTest
    {
        [SetUp]
        public void SetUp()
        {
            comparer = new ColumnFamilyEqualityByPropertiesComparer();
        }

        [Test]
        public void TestCompareByReadRepairChanceProperty()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", ReadRepairChance = 1.0}, new ColumnFamily {Name = "123", ReadRepairChance = 1.0})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", ReadRepairChance = 1.0}, new ColumnFamily {Name = "123", ReadRepairChance = 1.1})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", ReadRepairChance = 1.0}, new ColumnFamily {Name = "123", ReadRepairChance = null})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "123", ReadRepairChance = null},
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.2}
                )
            );
        }

        [Test]
        public void TestCompareByGCGraceSecondsProperty()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", GCGraceSeconds = 1}, new ColumnFamily {Name = "123", GCGraceSeconds = 1})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", GCGraceSeconds = 1}, new ColumnFamily {Name = "123", GCGraceSeconds = 2})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", GCGraceSeconds = null}, new ColumnFamily {Name = "123", GCGraceSeconds = 2})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", GCGraceSeconds = 2}, new ColumnFamily {Name = "123", GCGraceSeconds = null})
            );
        }

        [Test]
        public void TestTryCompareColumnFamiliesWithDifferentNames()
        {
            Assert.Throws<InvalidOperationException>(
                () => comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "name", GCGraceSeconds = 1}, new ColumnFamily {Name = "wrong_name", GCGraceSeconds = 1})
            );
        }

        [Test]
        public void TestCompareColumnFamiliesByCompactionStrategy()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(10)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(10)})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategyDisabled()},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategyDisabled()})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 32)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 32)})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategyDisabled()},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategyDisabled()})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 32)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategyDisabled()})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 32)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(16, 32)})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 32)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 16)})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(20)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(10)})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(4)},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 4)})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategyDisabled()},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategyDisabled()})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategyDisabled()},
                    new ColumnFamily {Name = "name", CompactionStrategy = null})
            );
        }

        [Test]
        public void TestCompareColumnFamiliesByCaching()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.All},
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.All})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.None},
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.All})
            );
        }

        [Test]
        public void TestCompareColumnFamiliesByCompression()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 3})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 2})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 2})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.LZ4(new CompressionOptions {ChunkLengthInKb = 2})})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.None()},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = null},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2})},
                    new ColumnFamily {Name = "name", Compression = null})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(
                    new ColumnFamily {Name = "name", Compression = null},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Snappy(null)})
            );
        }

        [Test]
        public void TestCompareByBloomFilterFpChanceProperty()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", BloomFilterFpChance = 0.1}, new ColumnFamily {Name = "123", BloomFilterFpChance = 0.1})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", BloomFilterFpChance = 0.1}, new ColumnFamily {Name = "123", BloomFilterFpChance = 0.2})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", BloomFilterFpChance = null}, new ColumnFamily {Name = "123", BloomFilterFpChance = 0.2})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", BloomFilterFpChance = 0.2}, new ColumnFamily {Name = "123", BloomFilterFpChance = null})
            );
        }

        [Test]
        public void TestCompareByDefaultTtlProperty()
        {
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", DefaultTtl = 1}, new ColumnFamily {Name = "123", DefaultTtl = 1})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", DefaultTtl = 1}, new ColumnFamily {Name = "123", DefaultTtl = 2})
            );
            Assert.That(
                !comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", DefaultTtl = null}, new ColumnFamily {Name = "123", DefaultTtl = 2})
            );
            Assert.That(
                comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", DefaultTtl = 2}, new ColumnFamily {Name = "123", DefaultTtl = null})
            );
        }

        [Test]
        public void TestTryCompareColumnFamiliesWithDifferentComparatorTypes()
        {
            Assert.Throws<InvalidOperationException>(() => comparer.NeedUpdateColumnFamily(new ColumnFamily {Name = "123", ComparatorType = new ColumnComparatorType(DataType.Int32Type)}, new ColumnFamily {Name = "123", ComparatorType = new ColumnComparatorType(DataType.BytesType)}));
        }

        private ColumnFamilyEqualityByPropertiesComparer comparer;
    }
}