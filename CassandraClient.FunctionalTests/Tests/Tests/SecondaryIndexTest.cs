using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class SecondaryIndexTest : CassandraFunctionalTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            var indexes = new List<IndexDefinition>
                {
                    new IndexDefinition
                        {Name = "col1", ValidationClass = DataType.LongType},
                    new IndexDefinition
                        {Name = "col2", ValidationClass = DataType.UTF8Type},
                    new IndexDefinition
                        {Name = "col3", ValidationClass = DataType.UTF8Type},
                    new IndexDefinition
                        {
                            Name = "col4", ValidationClass = DataType.LongType
                        }
                };

            cassandraCluster.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
                        {
                            Name = KeyspaceName,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = Constants.ColumnFamilyName,
                                                    Indexes = indexes
                                                }
                                        },
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(1)
                                }
                        }
                });

            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            for(int i = 0; i < count; i++)
            {
                var columns = new List<Column>
                    {
                        new Column
                            {
                                Name = "col1",
                                Value = BitConverter.GetBytes((long)i / 10)
                            },
                        new Column
                            {
                                Name = "col2",
                                Value = Encoding.UTF8.GetBytes(i.ToString())
                            },
                        new Column
                            {
                                Name = "col3",
                                Value = Encoding.UTF8.GetBytes("zzz")
                            },
                        new Column
                            {
                                Name = "col4",
                                Value = BitConverter.GetBytes((long)i)
                            }
                    };
                conn.AddBatch(i.ToString(), columns);
            }
        }

        [Test]
        public void TestOneIndexEQ()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            {
                for(int i = 0; i < count; i += 10)
                {
                    var res = conn.GetRowsWithColumnValue(1000, "col1", BitConverter.GetBytes((long)i / 10)).OrderBy(s => s).ToArray();
                    Assert.AreEqual(10, res.Length);
                    for(int j = 0; j < 10; j++)
                        Assert.AreEqual((i + j).ToString(), res[j]);
                }
            }
        }

        [Test]
        public void TestOneIndexRange()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);

            string[] res = conn.GetRowsWhere(null, 1000, new[]
                {
                    new IndexExpression
                        {
                            ColumnName = "col1",
                            IndexOperator = IndexOperator.GTE,
                            Value = BitConverter.GetBytes(3L)
                        },
                    new IndexExpression
                        {
                            ColumnName = "col1",
                            IndexOperator = IndexOperator.LT,
                            Value = BitConverter.GetBytes(8L)
                        },
                    new IndexExpression
                        {
                            ColumnName = "col3",
                            IndexOperator = IndexOperator.EQ,
                            Value = Encoding.UTF8.GetBytes("zzz")
                        }
                }, new[] {"col1"}).OrderBy(s => s).ToArray();
            Assert.AreEqual(50, res.Length);
            for(int i = 0; i < 50; i++)
                Assert.That(res[i], Is.EqualTo((3 * 10 + i).ToString()));
        }

        [Test]
        public void TestTwoIndexes()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            string[] res = conn.GetRowsWhere(null, 1000, new[]
                {
                    new IndexExpression
                        {
                            ColumnName = "col1",
                            IndexOperator = IndexOperator.EQ,
                            Value = BitConverter.GetBytes(3L)
                        },
                    new IndexExpression
                        {
                            ColumnName = "col2",
                            IndexOperator = IndexOperator.EQ,
                            Value = Encoding.UTF8.GetBytes("32")
                        }
                }, new[] {"col1"});
            Assert.AreEqual(1, res.Length);
            Assert.That(res[0], Is.EqualTo("32"));
        }

        [Test]
        public void TestTimedIndex()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            string[] res = conn.GetRowsWhere(null, 1000, new[]
                {
                    new IndexExpression
                        {
                            ColumnName = "col3",
                            IndexOperator = IndexOperator.EQ,
                            Value = Encoding.UTF8.GetBytes("zzz")
                        },
                    new IndexExpression
                        {
                            ColumnName = "col4",
                            IndexOperator = IndexOperator.GT,
                            Value = BitConverter.GetBytes(10L)
                        }
                }, new[] {"col1"});
            foreach(var re in res)
                Console.WriteLine(re);
            Assert.AreEqual(89, res.Length);
            for(int i = 11; i < count; i++)
                Assert.That(res.Contains("" + i));
        }

        private const int count = 100;
    }
}