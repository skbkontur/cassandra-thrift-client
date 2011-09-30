using CassandraClient.Abstractions;
using CassandraClient.StorageCore;
using CassandraClient.StorageCore.RowsStorage;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.StorageCoreTests
{
    public class ObjectReaderTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            collection = GetMock<IVersionReaderCollection>();
            objectReader = new ObjectReader(collection);
        }

        [Test]
        public void TestNoSpecialColumns()
        {
            var versionReader = GetMock<IVersionReader>();
            collection.Expect(readerCollection => readerCollection.GetVersionReader("v1")).Return(versionReader);
            var expected = new TestClass {Property = "property"};
            versionReader.Expect(reader => reader.TryReadObject(ARG.EqualsTo(new Column[0]), ARG.EqualsTo(new Column[0]), out ARG.Out(expected).Dummy)).Return(true);
            TestClass actual;
            Assert.AreEqual(true, objectReader.TryReadObject(new Column[0], out actual));
            actual.AssertEqualsTo(expected);
        }

        [Test]
        public void TestSpecialColumns()
        {
            var columns = new[]
                {
                    CreateColumn("name1", "value1"),
                    CreateColumn(SerializeToRowsStorageConstants.formatVersionColumnName, "versionZZZ"),
                    CreateColumn("name2", "value2"),
                    CreateColumn(SerializeToRowsStorageConstants.fullObjectColumnName, "fullObject"),
                    CreateColumn("name3", "value3"),
                    CreateColumn(SerializeToRowsStorageConstants.idColumnName, "iddd")
                };
            var specialColumns = new[]
                {
                    CreateColumn(SerializeToRowsStorageConstants.formatVersionColumnName, "versionZZZ"),
                    CreateColumn(SerializeToRowsStorageConstants.fullObjectColumnName, "fullObject"),
                    CreateColumn(SerializeToRowsStorageConstants.idColumnName, "iddd")
                };

            var versionReader = GetMock<IVersionReader>();
            collection.Expect(readerCollection => readerCollection.GetVersionReader("versionZZZ")).Return(versionReader);
            var expected = new TestClass {Property = "property"};
            versionReader.Expect(reader => reader.TryReadObject(ARG.EqualsTo(columns), ARG.EqualsTo(specialColumns), out ARG.Out(expected).Dummy)).Return(true);
            TestClass actual;
            Assert.AreEqual(true, objectReader.TryReadObject(columns, out actual));
            actual.AssertEqualsTo(expected);
        }

        [Test]
        public void TestFalseResult()
        {
            var columns = new[]
                {
                    CreateColumn("name1", "value1"),
                    CreateColumn(SerializeToRowsStorageConstants.formatVersionColumnName, "versionZZZ"),
                    CreateColumn("name2", "value2"),
                    CreateColumn(SerializeToRowsStorageConstants.fullObjectColumnName, "fullObject"),
                    CreateColumn("name3", "value3"),
                    CreateColumn(SerializeToRowsStorageConstants.idColumnName, "iddd")
                };
            var specialColumns = new[]
                {
                    CreateColumn(SerializeToRowsStorageConstants.formatVersionColumnName, "versionZZZ"),
                    CreateColumn(SerializeToRowsStorageConstants.fullObjectColumnName, "fullObject"),
                    CreateColumn(SerializeToRowsStorageConstants.idColumnName, "iddd")
                };

            var versionReader = GetMock<IVersionReader>();
            collection.Expect(readerCollection => readerCollection.GetVersionReader("versionZZZ")).Return(versionReader);
            var expected = new TestClass { Property = "property" };
            versionReader.Expect(reader => reader.TryReadObject(ARG.EqualsTo(columns), ARG.EqualsTo(specialColumns), out ARG.Out(expected).Dummy)).Return(false);
            TestClass actual;
            Assert.AreEqual(false, objectReader.TryReadObject(columns, out actual));
            actual.AssertEqualsTo(expected);
        }

        [Test]
        public void TestNullColumns()
        {
            var versionReader = GetMock<IVersionReader>();
            collection.Expect(readerCollection => readerCollection.GetVersionReader("v1")).Return(versionReader);
            var expected = new TestClass { Property = "property" };
            versionReader.Expect(reader => reader.TryReadObject(ARG.EqualsTo(new Column[0]), ARG.EqualsTo(new Column[0]), out ARG.Out(expected).Dummy)).Return(true);
            TestClass actual;
            Assert.AreEqual(true, objectReader.TryReadObject(null, out actual));
            actual.AssertEqualsTo(expected);
        }

        public class TestClass
        {
            public string Property { get; set; }
        }

        private Column CreateColumn(string name, string value)
        {
            return new Column
                {
                    Name = name,
                    Value = CassandraStringHelpers.StringToBytes(value)
                };
        }

        private IVersionReaderCollection collection;
        private ObjectReader objectReader;
    }
}