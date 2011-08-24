using CassandraClient.Abstractions;
using CassandraClient.StorageCore.RowsStorage;

using GroboSerializer;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.StorageCoreTests
{
    public class Version2ReaderTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            serializer = GetMock<ISerializer>();
            version2Reader = new Version2Reader(serializer);
        }

        [Test]
        public void TestNoFullObjectColumn()
        {
            TestClass result;
            Assert.IsFalse(version2Reader.TryReadObject(new Column[0], new Column[0], out result));
            Assert.IsNull(result);
        }

        [Test]
        public void TestCorrect()
        {
            TestClass result;
            var columns = new[]
                {
                    new Column {Name = "someColumn", Value = new byte[0]},
                    new Column {Name = SerializeToRowsStorageConstants.fullObjectColumnName, Value = new byte[]{1,2,3,4,5}},
                };
            var expected = new TestClass { Z = "q" };
            serializer.Expect(s => s.Deserialize<TestClass>(new byte[] {1, 2, 3, 4, 5})).Return(expected);
            Assert.IsTrue(version2Reader.TryReadObject(new Column[0], columns, out result));
            result.AssertEqualsTo(expected);
        }

        public class TestClass
        {
            public string Z { get; set; }
        }

        private ISerializer serializer;
        private Version2Reader version2Reader;
    }
}