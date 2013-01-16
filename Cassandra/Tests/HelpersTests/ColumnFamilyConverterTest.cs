using System.Collections.Generic;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Encoders;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using NUnit.Framework;

namespace Cassandra.Tests.HelpersTests
{
    public class ColumnFamilyConverterTest : TestBase
    {
        [Test]
        public void TestNull()
        {
            Assert.IsNull(((ColumnFamily)null).ToAquilesColumnFamily(null));
            Assert.IsNull(((AquilesColumnFamily)null).ToColumnFamily());
        }

        [Test]
        public void TestToAquilesColumnFamilyNoIndexDefinitions()
        {
            var columnFamily = new ColumnFamily
                {
                    Name = "testName"
                };
            var expectedAquilesColumnFamily = new AquilesColumnFamily
                {
                    Name = "testName",
                    Keyspace = "testKeyspace",
                    Comparator = "UTF8Type"
                };
            columnFamily.ToAquilesColumnFamily("testKeyspace").AssertEqualsTo(expectedAquilesColumnFamily);
        }

        [Test]
        public void TestToAquilesColumnFamilyWithIndexDefinitions()
        {
            var columnFamily = new ColumnFamily
                {
                    Name = "testName",
                    Indexes = new List<IndexDefinition>
                        {
                            new IndexDefinition
                                {
                                    Name = "name1",
                                    ValidationClass = ValidationClass.UTF8Type
                                },
                            new IndexDefinition
                                {
                                    Name = "name2",
                                    ValidationClass = ValidationClass.LongType
                                }
                        },
                    Id = 3434
                };
            var expectedAquilesColumnFamily = new AquilesColumnFamily
                {
                    Name = "testName",
                    Keyspace = "testKeyspace",
                    Comparator = "UTF8Type",
                    Columns = new List<IndexDefinition>
                        {
                            new IndexDefinition
                                {
                                    Name = "name1",
                                    ValidationClass = ValidationClass.UTF8Type
                                },
                            new IndexDefinition
                                {
                                    Name = "name2",
                                    ValidationClass = ValidationClass.LongType
                                }
                        },
                    Id = 3434
                };
            columnFamily.ToAquilesColumnFamily("testKeyspace").AssertEqualsTo(expectedAquilesColumnFamily);
        }

        [Test]
        public void TestToColumnFamilyNoColumnDefinitions()
        {
            var aquilesColumnFamily = new AquilesColumnFamily
                {
                    Name = "testName",
                    Keyspace = "testKeyspace",
                    Id = 5
                };
            var expectedColumnFamily = new ColumnFamily
                {
                    Name = "testName",
                    Id = 5
                };
            aquilesColumnFamily.ToColumnFamily().AssertEqualsTo(expectedColumnFamily);
        }

        [Test]
        public void TestToColumnFamilyWithColumnDefinitions()
        {
            var aquilesColumnFamily = new AquilesColumnFamily
                {
                    Name = "testName",
                    Keyspace = "testKeyspace",
                    Columns = new List<IndexDefinition>
                        {
                            new IndexDefinition
                                {
                                    Name = "name1",
                                    ValidationClass = ValidationClass.UTF8Type
                                }
                        }
                };
            var expectedColumnFamily = new ColumnFamily
                {
                    Name = "testName",
                    Indexes = new List<IndexDefinition>
                        {
                            new IndexDefinition
                                {
                                    Name = "name1",
                                    ValidationClass = ValidationClass.UTF8Type
                                }
                        }
                };
            aquilesColumnFamily.ToColumnFamily().AssertEqualsTo(expectedColumnFamily);
        }
    }
}