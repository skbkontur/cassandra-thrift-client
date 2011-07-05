using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using Cassandra.Tests;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.StorageCore.RowsStorage;

using GroboSerializer;

using NUnit.Framework;

using Tests.Tests;

namespace Tests.StorageCoreTests
{
    public class SyncListTest : TestBase
    {
        #region Setup/Teardown

        public override void SetUp()
        {
            base.SetUp();
            cassandraCluster = new CassandraCluster(new CassandraClusterSettings
                {
                    ClusterReadConsistencyLevel = ConsistencyLevel.ALL,
                    ClusterWriteConsistencyLevel = ConsistencyLevel.ALL,
                    ColumnFamilyReadConsistencyLevel = ConsistencyLevel.QUORUM,
                    ColumnFamilyWriteConsistencyLevel = ConsistencyLevel.QUORUM,
                    Name = Constants.ClusterName
                });

            serializer = new Serializer(new TestXmlNamespaceFactory());
            var columnFamilyRegistry = new TestColumnFamilyRegistry(serializer);
            CassandraInitializer.CreateNewKeyspace(cassandraCluster, columnFamilyRegistry);

            var cassandraCoreSettings = new TestCassandraCoreSettings();
            storage = new SerializeToRowsStorage(columnFamilyRegistry, columnFamilyRegistry, cassandraCluster, cassandraCoreSettings,
                                                 serializer);
        }

        #endregion

        [Test]
        public void TestSearch()
        {
            const int count = 100;
            var elements = new List<TestStorageElement>();
            var rnd = new Random();
            for(int i = 0; i < count; ++i)
            {
                int a = rnd.Next(2);
                int b = rnd.Next(2);
                var c = rnd.Next(2);
                var d = rnd.Next(2);
                var element = new TestStorageElement
                    {
                        Id = Guid.NewGuid().ToString(),
                        StringProperty = "String" + a,
                        IntProperty = 1234 + b,
                        ComplexProperty = new TestStorageElementSubItem
                            {
                                StringProperty = "String" + c,
                                IntProperty = 4321 + d,
                            }
                    };
                elements.Add(element);
            }
            storage.Write(elements.Select(e => new KeyValuePair<string, TestStorageElement>(e.Id, e)).ToArray());
            for(int a = 0; a < 3; ++a)
            {
                for(int b = 0; b < 3; ++b)
                {
                    for(int c = 0; c < 3; ++c)
                    {
                        for(int d = 0; d < 3; ++d)
                        {
                            if(a == 0 && b == 0 && c == 0 && d == 0)
                                continue;
                            var query = new TestStorageElementSearchQuery
                                {
                                    StringProperty = a == 0 ? null : "String" + (a - 1),
                                    IntProperty = b == 0 ? null : (int?)(1234 + (b - 1)),
                                    ComplexProperty =
                                        c == 0 && d == 0
                                            ? null
                                            : new TestStorageElementSubItem
                                                {
                                                    StringProperty = c == 0 ? null : "String" + (c - 1),
                                                    IntProperty = d == 0 ? null : (int?)(4321 + (d - 1)),
                                                }
                                };
                            var actual =
                                storage.Read<TestStorageElement>(
                                    storage.Search<TestStorageElement, TestStorageElementSearchQuery>(query));
                            Array.Sort(actual, (first, second) => first.Id.CompareTo(second.Id));
                            var expected = Search(elements, query);
                            Array.Sort(expected, (first, second) => first.Id.CompareTo(second.Id));

                            Assert.AreEqual(expected.Length, actual.Length);
                            for(var i = 0; i < expected.Length; ++i)
                            {
                                Assert.AreEqual(serializer.SerializeToString(expected[i], false, Encoding.UTF8),
                                                serializer.SerializeToString(actual[i], false, Encoding.UTF8));
                            }
                        }
                    }
                }
            }
        }

        private static bool Matches(TestStorageElement element, TestStorageElementSearchQuery query)
        {
            if(query.StringProperty != null && element.StringProperty != query.StringProperty)
                return false;
            if(query.IntProperty != null && element.IntProperty != query.IntProperty)
                return false;
            if(query.ComplexProperty != null)
            {
                if(query.ComplexProperty.StringProperty != null &&
                   element.ComplexProperty.StringProperty != query.ComplexProperty.StringProperty)
                    return false;
                if(query.ComplexProperty.IntProperty != null &&
                   element.ComplexProperty.IntProperty != query.ComplexProperty.IntProperty)
                    return false;
            }
            return true;
        }

        private static TestStorageElement[] Search(List<TestStorageElement> elements,
                                                   TestStorageElementSearchQuery query)
        {
            return elements.Where(element => Matches(element, query)).ToArray();
        }

        private SerializeToRowsStorage storage;
        private Serializer serializer;
        private CassandraCluster cassandraCluster;
    }
}