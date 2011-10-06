using System.Collections.Specialized;

using CassandraClient.Abstractions;
using CassandraClient.Helpers;

using GroboSerializer;

using NUnit.Framework;

using Rhino.Mocks;

using StorageCore;
using StorageCore.RowsStorage;

namespace Cassandra.Tests.StorageCoreTests
{
    public class Version1ReaderTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            serializer = GetMock<ISerializer>();
            version1Reader = new Version1Reader(serializer);
        }

        [Test]
        public void TestNoColumns()
        {
            TestClass result;
            Assert.IsFalse(version1Reader.TryReadObject(new Column[0], new Column[0], out result));
            Assert.IsNull(result);
        }

        [Test]
        public void TestCorrect()
        {
            TestClass result;
            var nvc = new NameValueCollection
                {
                    {"A", "B"},
                    {"C", "D"}
                };
            var columns = new[]
                {
                    new Column {Name = "C", Value = StringHelpers.StringToBytes("D")},
                    new Column {Name = "A", Value = StringHelpers.StringToBytes("B")},
                };
            var expected = new TestClass {TestProperty = "tp"};
            serializer.Expect(s => s.Deserialize<TestClass>(ARG.EqualsTo(nvc))).Return(expected);
            Assert.IsTrue(version1Reader.TryReadObject(columns, new Column[0], out result));
            result.AssertEqualsTo(expected);
        }

        public class TestClass
        {
            public string TestProperty { get; set; }
        }

        private ISerializer serializer;
        private Version1Reader version1Reader;
    }
}