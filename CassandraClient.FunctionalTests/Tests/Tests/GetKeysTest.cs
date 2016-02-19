using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class GetKeysTest : CassandraFunctionalTestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            cassandraCluster.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
                        {
                            Name = KeyspaceName,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(1),
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = Constants.ColumnFamilyName
                                                }
                                        }
                                }
                        }
                });
        }

        [Test]
        public void TestEmpty()
        {
            CollectionAssert.IsEmpty(cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName)
                                         .GetKeys());
        }

        [Test]
        public void TestOneKey()
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            var key = Guid.NewGuid().ToString();
            connection.AddColumn(key, new Column
                {
                    Name = Guid.NewGuid().ToString(),
                    Value = new byte[] {1, 2, 3}
                });
            var keys = connection.GetKeys().ToArray();
            CollectionAssert.AreEqual(new[] {key}, keys);
        }

        [Test]
        public void TestManyKeys()
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            var keys = new List<string>();
            for(int i = 0; i < 100; i++)
            {
                var key = Guid.NewGuid().ToString();
                connection.AddColumn(key, new Column
                    {
                        Name = Guid.NewGuid().ToString(),
                        Value = new byte[] {1, 2, 3}
                    });
                keys.Add(key);
            }

            CollectionAssert.AreEqual(keys.OrderBy(s => s).ToArray(), connection.GetKeys(3).OrderBy(s => s).ToArray());
        }

        [Test]
        public void TestExclusiveStartKey()
        {
            var connection = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            var keys = new List<string>();
            for(var i = 0; i < 100; i++)
            {
                var key = Guid.NewGuid().ToString();
                connection.AddColumn(key, new Column
                    {
                        Name = Guid.NewGuid().ToString(),
                        Value = new byte[] {1, 2, 3}
                    });
                keys.Add(key);
            }

            var actualKeys = new List<string>();
            string exclusiveStartKey = null;
            for(var i = 0; i < 25; i++)
            {
                var nextBatch = connection.GetKeys(exclusiveStartKey, 4);
                Assert.AreEqual(4, nextBatch.Length);
                exclusiveStartKey = nextBatch.Last();
                actualKeys.AddRange(nextBatch);
            }
            CollectionAssert.AreEqual(keys.OrderBy(s => s).ToArray(), actualKeys.OrderBy(s => s).ToArray());
        }
    }
}