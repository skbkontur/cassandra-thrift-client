using System;
using System.Collections.Generic;
using System.Linq;

using CassandraClient.Abstractions;
using CassandraClient.AquilesTrash.Encoders;
using CassandraClient.Connections;

using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class SecondaryIndexTest : CassandraFunctionalTestBase
    {
        #region Setup/Teardown

        public override void SetUp()
        {
            base.SetUp();
            var cassandraClient = new CassandraClient(cassandraCluster);
            cassandraClient.RemoveAllKeyspaces();

            var indexes = new List<IndexDefinition>
                {
                    new IndexDefinition
                        {Name = "col1", ValidationClass = ValidationClass.LongType},
                    new IndexDefinition
                        {Name = "col2", ValidationClass = ValidationClass.UTF8Type},
                    new IndexDefinition
                        {Name = "col3", ValidationClass = ValidationClass.UTF8Type},
                    new IndexDefinition
                        {
                            Name = "col4", ValidationClass = ValidationClass.LongType
                        }
                };

            using(IClusterConnection clusterConnection = cassandraCluster.RetrieveClusterConnection())
            {
                clusterConnection.AddKeyspace(new Keyspace
                    {
                        ColumnFamilies = new Dictionary<string, ColumnFamily>
                            {
                                {
                                    Constants.ColumnFamilyName, new ColumnFamily
                                        {
                                            Name = Constants.ColumnFamilyName,
                                            Indexes = indexes
                                        }
                                    }
                            },
                        Name = Constants.KeyspaceName,
                        ReplicaPlacementStrategy =
                            "org.apache.cassandra.locator.SimpleStrategy",
                        ReplicationFactor = 1
                    });
            }

            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                for(int i = 0; i < count; i++)
                {
                    var columns = new List<Column>
                        {
                            new Column
                                {
                                    Name = "col1",
                                    Value = ByteEncoderHelper.LongEncoder.ToByteArray(i / 10)
                                },
                            new Column
                                {
                                    Name = "col2",
                                    Value = ByteEncoderHelper.UTF8Encoder.ToByteArray(i.ToString())
                                },
                            new Column
                                {
                                    Name = "col3",
                                    Value = ByteEncoderHelper.UTF8Encoder.ToByteArray("zzz")
                                },
                            new Column
                                {
                                    Name = "col4",
                                    Value = ByteEncoderHelper.LongEncoder.ToByteArray(i)
                                }
                        };
                    conn.AddBatch(i.ToString(), columns);
                }
            }
        }

        #endregion

        [Test]
        public void TestOneIndexEQ()
        {
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                for(int i = 0; i < count; i += 10)
                {
                    var res =
                        conn.GetRowsWithColumnValue(1000, "col1", ByteEncoderHelper.LongEncoder.ToByteArray(i / 10)).
                            OrderBy(s => s).ToArray();
                    Assert.AreEqual(10, res.Length);
                    for(int j = 0; j < 10; j++)
                        Assert.AreEqual((i + j).ToString(), res[j]);
                }
            }
        }

        [Test]
        public void TestOneIndexRange()
        {
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                string[] res = conn.GetRowsWhere(null, 1000, new[]
                    {
                        new IndexExpression
                            {
                                ColumnName = "col1",
                                IndexOperator = IndexOperator.GTE,
                                Value =
                                    ByteEncoderHelper.LongEncoder.ToByteArray(3)
                            },
                        new IndexExpression
                            {
                                ColumnName = "col1",
                                IndexOperator = IndexOperator.LT,
                                Value =
                                    ByteEncoderHelper.LongEncoder.ToByteArray(8)
                            },
                        new IndexExpression
                            {
                                ColumnName = "col3",
                                IndexOperator = IndexOperator.EQ,
                                Value =
                                    ByteEncoderHelper.UTF8Encoder.ToByteArray(
                                        "zzz")
                            }
                    }, new[] {"col1"}).OrderBy(s => s).ToArray();
                Assert.AreEqual(50, res.Length);
                for(int i = 0; i < 50; i++)
                    Assert.That(res[i], Is.EqualTo((3 * 10 + i).ToString()));
            }
        }

        [Test]
        public void TestTwoIndexes()
        {
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                string[] res = conn.GetRowsWhere(null, 1000, new[]
                    {
                        new IndexExpression
                            {
                                ColumnName = "col1",
                                IndexOperator = IndexOperator.EQ,
                                Value =
                                    ByteEncoderHelper.LongEncoder.ToByteArray
                                    (3)
                            },
                        new IndexExpression
                            {
                                ColumnName = "col2",
                                IndexOperator = IndexOperator.EQ,
                                Value =
                                    ByteEncoderHelper.UTF8Encoder.ToByteArray
                                    ("32")
                            }
                    }, new[] {"col1"});
                Assert.AreEqual(1, res.Length);
                Assert.That(res[0], Is.EqualTo("32"));
            }
        }

        [Test]
        public void TestTimedIndex()
        {
            using(var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                string[] res = conn.GetRowsWhere(null, 1000, new[]
                    {
                        new IndexExpression
                            {
                                ColumnName = "col3",
                                IndexOperator = IndexOperator.EQ,
                                Value = ByteEncoderHelper.UTF8Encoder.ToByteArray("zzz")
                            },
                        new IndexExpression
                            {
                                ColumnName = "col4",
                                IndexOperator = IndexOperator.GT,
                                Value = ByteEncoderHelper.LongEncoder.ToByteArray(10)
                            }
                    }, new[] {"col1"});
                foreach(var re in res)
                    Console.WriteLine(re);
                Assert.AreEqual(89, res.Length);
                for(int i = 11; i < count; i++)
                    Assert.That(res.Contains("" + i));
            }
        }

        private const int count = 100;
    }
}