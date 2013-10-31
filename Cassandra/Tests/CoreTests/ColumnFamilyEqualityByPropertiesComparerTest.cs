using System;
using System.Collections.Generic;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace Cassandra.Tests.CoreTests
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
                comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0},
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0}
                    )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0},
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.1}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 1.0},
                    new ColumnFamily {Name = "123", ReadRepairChance = null}
                     )
                );
        }

        [Test]
        public void TestCompareByGCGraceSecondsProperty()
        {
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "123", GCGraceSeconds = 1},
                    new ColumnFamily {Name = "123", GCGraceSeconds = 1}
                    )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 1},
                    new ColumnFamily {Name = "123", ReadRepairChance = 2}
                     )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = null},
                    new ColumnFamily {Name = "123", ReadRepairChance = 2}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "123", ReadRepairChance = 2},
                    new ColumnFamily {Name = "123", ReadRepairChance = null}
                     )
                );
        }

        [Test]
        public void TestTryCompareColumnFamiliesWithDifferentNames()
        {
            Assert.Throws<InvalidOperationException>(
                () => comparer.Equals(
                    new ColumnFamily {Name = "name", GCGraceSeconds = 1},
                    new ColumnFamily {Name = "wrong_name", GCGraceSeconds = 1}
                          )
                );
        }

        [Test]
        public void TestCompareColumnFamiliesByCompactionStrategy()
        {
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 10})},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 10})}
                    )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions())},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions())}
                    )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy()},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy()}
                    )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 20})},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 10})}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions())},
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy()}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions())},
                    new ColumnFamily {Name = "name", CompactionStrategy = null}
                     )
                );
        }

        [Test]
        public void TestCompareColumnFamiliesByCaching()
        {
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.All},
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.All}
                    )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.None},
                    new ColumnFamily {Name = "name", Caching = ColumnFamilyCaching.All}
                    )
                );
        }

        [Test]
        public void TestCompareColumnFamiliesByCompression()
        {
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 1.0})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 1.0})}
                    )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 3, CrcCheckChance = 1.0})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 1.0})}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 1.0})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 2.0})}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 1.0})},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 2.0})}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.None()},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 2.0})}
                     )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = null},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 2.0})}
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 2.0})},
                    new ColumnFamily {Name = "name", Compression = null}
                     )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily {Name = "name", Compression = null},
                    new ColumnFamily {Name = "name", Compression = ColumnFamilyCompression.Snappy(null)}
                    )
                );
        }

        [Test]
        public void TestCompareColumnFamiliesByIndexes()
        {
            Assert.That(
                comparer.Equals(
                    new ColumnFamily
                        {
                            Name = "name", Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column", ValidationClass = DataType.BooleanType}
                                }
                        },
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column", ValidationClass = DataType.BooleanType}
                                }
                        }
                    )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily
                        {
                            Name = "name", Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column1", ValidationClass = DataType.BooleanType},
                                    new IndexDefinition {Name = "column2", ValidationClass = DataType.BooleanType}
                                }
                        },
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column1", ValidationClass = DataType.BooleanType},
                                    new IndexDefinition {Name = "column2", ValidationClass = DataType.BooleanType}
                                }
                        }
                    )
                );
            Assert.That(
                comparer.Equals(
                    new ColumnFamily
                        {
                            Name = "name", Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column2", ValidationClass = DataType.BooleanType},
                                    new IndexDefinition {Name = "column1", ValidationClass = DataType.BooleanType}
                                }
                        },
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column1", ValidationClass = DataType.BooleanType},
                                    new IndexDefinition {Name = "column2", ValidationClass = DataType.BooleanType}
                                }
                        }
                    )
                );

            Assert.That(
                !comparer.Equals(
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column", ValidationClass = DataType.BooleanType}
                                }
                        },
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column2", ValidationClass = DataType.BooleanType}
                                }
                        }
                     )
                );
            Assert.That(
                !comparer.Equals(
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column", ValidationClass = DataType.DateType}
                                }
                        },
                    new ColumnFamily
                        {
                            Name = "name",
                            Indexes = new List<IndexDefinition>
                                {
                                    new IndexDefinition {Name = "column", ValidationClass = DataType.BooleanType}
                                }
                        }
                    )
                );
        }

        private ColumnFamilyEqualityByPropertiesComparer comparer;
    }
}