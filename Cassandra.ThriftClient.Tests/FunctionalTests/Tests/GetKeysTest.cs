using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class GetKeysTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestEmpty()
        {
            CollectionAssert.IsEmpty(columnFamilyConnection.GetKeys());
        }

        [Test]
        public void TestOneKey()
        {
            var key = Guid.NewGuid().ToString();
            columnFamilyConnection.AddColumn(key, new Column
                {
                    Name = Guid.NewGuid().ToString(),
                    Value = new byte[] {1, 2, 3}
                });
            var keys = columnFamilyConnection.GetKeys().ToArray();
            CollectionAssert.AreEqual(new[] {key}, keys);
        }

        [Test]
        public void TestManyKeys()
        {
            var keys = new List<string>();
            for (var i = 0; i < 100; i++)
            {
                var key = Guid.NewGuid().ToString();
                columnFamilyConnection.AddColumn(key, new Column
                    {
                        Name = Guid.NewGuid().ToString(),
                        Value = new byte[] {1, 2, 3}
                    });
                keys.Add(key);
            }

            CollectionAssert.AreEqual(keys.OrderBy(s => s).ToArray(), columnFamilyConnection.GetKeys(3).OrderBy(s => s).ToArray());
        }

        [Test]
        public void TestExclusiveStartKey()
        {
            var keys = new List<string>();
            for (var i = 0; i < 100; i++)
            {
                var key = Guid.NewGuid().ToString();
                columnFamilyConnection.AddColumn(key, new Column
                    {
                        Name = Guid.NewGuid().ToString(),
                        Value = new byte[] {1, 2, 3}
                    });
                keys.Add(key);
            }

            var actualKeys = new List<string>();
            string exclusiveStartKey = null;
            for (var i = 0; i < 25; i++)
            {
                var nextBatch = columnFamilyConnection.GetKeys(exclusiveStartKey, 4);
                Assert.AreEqual(4, nextBatch.Length);
                exclusiveStartKey = nextBatch.Last();
                actualKeys.AddRange(nextBatch);
            }
            CollectionAssert.AreEqual(keys.OrderBy(s => s).ToArray(), actualKeys.OrderBy(s => s).ToArray());
        }
    }
}