using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;

using Cassandra.Tests;

using CassandraClient.StorageCore.Exceptions;
using CassandraClient.StorageCore.RowsStorage;

using GroboSerializer;

using NUnit.Framework;

using Tests.Tests;

namespace Tests.StorageCoreTests
{
    public class SerializeToRowsStorageTest : CassandraFunctionalTestBase
    {
        #region Setup/Teardown

        public override void SetUp()
        {
            base.SetUp();

            serializer = new Serializer(new TestXmlNamespaceFactory());
            var columnFamilyRegistry = new TestColumnFamilyRegistry(serializer);
            CassandraInitializer.CreateNewKeyspace(cassandraCluster, columnFamilyRegistry);

            var cassandraCoreSettings = new TestCassandraCoreSettings();
            storage = new SerializeToRowsStorage(columnFamilyRegistry, columnFamilyRegistry, cassandraCluster, cassandraCoreSettings,
                                                 serializer, new ObjectReader(new VersionReaderCollection(serializer)));
        }

        #endregion

        [Test]
        public void TestUpdate()
        {
            var element1 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element1);
            var element2 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            storage.Write("zzz", element2);
            storage.Read<TestStorageElement>("zzz").AssertEqualsTo(element2);
        }

        [Test]
        public void TestMultiUpdate()
        {
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};

            //Thread.Sleep(1000);

            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.Read<TestStorageElement>("zzz").AssertEqualsTo(element12);
            storage.Read<TestStorageElement>("qxx").AssertEqualsTo(element22);
        }

        [Test]
        public void TestReadMultiple()
        {
            var element21 = new TestStorageElement { IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] { "arr2", "arr3" } };
            storage.Write("qxx", element21);
            var element11 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element11);
            var element12 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            var element22 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr4" } };

            //Thread.Sleep(1000);

            storage.Write(new[] { new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22) });
            storage.Read<TestStorageElement>(new[]{"zzz","qxx"}).AssertEqualsTo(new[]{element12,element22});
        }

        [Test]
        public void TestReadMultipleException()
        {
            var element11 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement { IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] { "arr2", "arr3" } };
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            var element22 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr4" } };
            storage.Write(new[] { new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22) });
            RunMethodWithException<StorageCoreException>(() => storage.Read<TestStorageElement>(new[] { "zzz", "qxx", "ttt" }), "Objects not found. Expected 3, but was 2");
        }

        [Test]
        public void TestReadMultipleEqualIdsInReadQuery()
        {
            var element11 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement { IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] { "arr2", "arr3" } };
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            var element22 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr4" } };

            //Thread.Sleep(1000);

            storage.Write(new[] { new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22) });
            RunMethodWithException<StorageCoreException>(() => storage.Read<TestStorageElement>(new[] { "zzz", "zzz", "zzz" }), "Objects not found. Expected 3, but was 1");
        }

        [Test]
        public void TestTryReadBadId()
        {
            TestStorageElement result;
            Assert.IsFalse(storage.TryRead("zzz",out result));
        }

        [Test]
        public void TestTryRead()
        {
            var element1 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element1);
            var element2 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            storage.Write("zzz", element2);
            TestStorageElement result;
            Assert.IsTrue(storage.TryRead("zzz",out result));
            result.AssertEqualsTo(element2);
        }

        [Test]
        public void TestTryReadMultiple()
        {
            var element11 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement { IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] { "arr2", "arr3" } };
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            var element22 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr4" } };

            //Thread.Sleep(1000);

            storage.Write(new[] { new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22) });
            storage.TryRead<TestStorageElement>(new[] { "zzz", "qxx" }).AssertEqualsTo(new[] { element12, element22 });
        }


        [Test]
        public void TestTryReadMultipleUnknownId()
        {
            var element11 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement { IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] { "arr2", "arr3" } };
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            var element22 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr4" } };

            //Thread.Sleep(1000);

            storage.Write(new[] { new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22) });
            storage.TryRead<TestStorageElement>(new[] { "zzz", "qxx", "ttt", "rrr" }).AssertEqualsTo(new[] { element12, element22 });
        }

        [Test]
        public void TestTryReadMultipleEqualIdsInReadQuery()
        {
            var element11 = new TestStorageElement { IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] { "arr0", "arr1" } };
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement { IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] { "arr2", "arr3" } };
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr2" } };
            var element22 = new TestStorageElement { IntProperty = null, StringProperty = null, Id = null, Arr = new[] { "arr4" } };

            //Thread.Sleep(1000);

            storage.Write(new[] { new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22) });
            storage.TryRead<TestStorageElement>(new[] { "zzz", "zzz", "zzz", "qxx", "qxx" }).AssertEqualsTo(new[] { element12, element12, element12, element22, element22 });
        }

        [Test]
        public void TestGetKeys()
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
            string prev = null;
            var allIds = new List<string>();
            for(;;)
            {
                var ids = storage.GetIds<TestStorageElement>(prev, 10);
                if(ids.Length == 0) break;
                allIds.AddRange(ids);
                prev = ids.Last();
            }
            Assert.AreEqual(allIds.Count, count);
            Assert.IsTrue(allIds.All(id => elements.Find(element => element.Id == id) != null));
        }

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
                                    storage.Search<TestStorageElement, TestStorageElementSearchQuery>(null, 1000, query));
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
    }
}