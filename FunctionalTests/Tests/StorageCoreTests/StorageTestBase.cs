using System;
using System.Collections.Generic;
using System.Linq;

using Cassandra.Tests;

using NUnit.Framework;

using SKBKontur.Cassandra.FunctionalTests.Tests;
using SKBKontur.Cassandra.StorageCore;
using SKBKontur.Cassandra.StorageCore.Exceptions;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public abstract class StorageTestBase : CassandraFunctionalTestBase
    {
        [Test]
        public void TestUpdate()
        {
            var storage = GetStorage();
            var element1 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element1);
            var element2 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            storage.Write("zzz", element2);
            storage.Read<TestStorageElement>("zzz").AssertEqualsTo(element2);
        }

        [Test]
        public void TestMultiUpdate()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.Read<TestStorageElement>("zzz").AssertEqualsTo(element12);
            storage.Read<TestStorageElement>("qxx").AssertEqualsTo(element22);
        }

        [Test]
        public void TestReadMultiple()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.Read<TestStorageElement>(new[] {"zzz", "qxx"}).AssertEqualsTo(new[] {element12, element22});
        }

        [Test]
        public void TestReadMultipleException()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            RunMethodWithException<StorageCoreException>(() => storage.Read<TestStorageElement>(new[] {"zzz", "qxx", "ttt"}), "Objects not found. Expected 3, but was 2");
        }

        [Test]
        public void TestReadMultipleEqualIdsInReadQuery()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.Read<TestStorageElement>(new[] {"zzz", "zzz", "zzz"}).AssertEqualsTo(new[] {element12, element12, element12});
        }

        [Test]
        public void TestTryReadBadId()
        {
            var storage = GetStorage();
            TestStorageElement result;
            Assert.IsFalse(storage.TryRead("zzz", out result));
        }

        [Test]
        public void TestTryRead()
        {
            var storage = GetStorage();
            var element1 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element1);
            var element2 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            storage.Write("zzz", element2);
            TestStorageElement result;
            Assert.IsTrue(storage.TryRead("zzz", out result));
            result.AssertEqualsTo(element2);
        }

        [Test]
        public void TestTryReadMultiple()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.TryRead<TestStorageElement>(new[] {"zzz", "qxx"}).AssertEqualsTo(new[] {element12, element22});
        }

        [Test]
        public void TestTryReadMultipleUnknownId()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.TryRead<TestStorageElement>(new[] {"zzz", "qxx", "ttt", "rrr"}).AssertEqualsTo(new[] {element12, element22});
        }

        [Test]
        public void TestTryReadMultipleEqualIdsInReadQuery()
        {
            var storage = GetStorage();
            var element11 = new TestStorageElement {IntProperty = 5, StringProperty = "zzz", Id = "id1", Arr = new[] {"arr0", "arr1"}};
            storage.Write("zzz", element11);
            var element21 = new TestStorageElement {IntProperty = 10, StringProperty = "qxx", Id = "id2", Arr = new[] {"arr2", "arr3"}};
            storage.Write("qxx", element21);
            var element12 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr2"}};
            var element22 = new TestStorageElement {IntProperty = null, StringProperty = null, Id = null, Arr = new[] {"arr4"}};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("zzz", element12), new KeyValuePair<string, TestStorageElement>("qxx", element22)});
            storage.TryRead<TestStorageElement>(new[] {"zzz", "zzz", "zzz", "qxx", "qxx"}).AssertEqualsTo(new[] {element12, element12, element12, element22, element22});
        }

        [Test]
        public void TestReadOrCreateWhenCreate()
        {
            var storage = GetStorage();
            storage.ReadOrCreate("zzz", (id) => new TestStorageElement {Id = id, IntProperty = 123})
                .AssertEqualsTo(new TestStorageElement {Id = "zzz", IntProperty = 123});
            storage.Read<TestStorageElement>("zzz").AssertEqualsTo(new TestStorageElement { Id = "zzz", IntProperty = 123 });
        }

        [Test]
        public void TestMultipleReadOrCreate()
        {
            var storage = GetStorage();
            var element1 = new TestStorageElement {IntProperty = 5, Id = "1"};
            var element2 = new TestStorageElement {IntProperty = 3, Id = "2"};
            storage.Write(new[] {new KeyValuePair<string, TestStorageElement>("1", element1), new KeyValuePair<string, TestStorageElement>("2", element2)});
            storage.ReadOrCreate(new[] {"1", "3", "2"}, (id) => new TestStorageElement
                {
                    Id = id,
                    IntProperty = 123
                }).AssertEqualsTo(new[] {element1, new TestStorageElement {Id = "3", IntProperty = 123}, element2});
            storage.Read<TestStorageElement>("3").AssertEqualsTo(new TestStorageElement { Id = "3", IntProperty = 123 });
        }

        [Test]
        public void TestGetKeys()
        {
            var storage = GetStorage();
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

        protected abstract IStorage GetStorage();
    }
}