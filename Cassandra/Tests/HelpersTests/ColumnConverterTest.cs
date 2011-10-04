using CassandraClient.AquilesTrash.Model;

using CassandraClient.Abstractions;
using CassandraClient.Helpers;

using NUnit.Framework;

namespace Cassandra.Tests.HelpersTests
{
    public class ColumnConverterTest : TestBase
    {
        [Test]
        public void TestNull()
        {
            Assert.IsNull(((Column)null).ToAquilesColumn());
            Assert.IsNull(((AquilesColumn)null).ToColumn());
        }

        [Test]
        public void TestToAquilesColumn()
        {
            var column = new Column
                {
                    Name = "djskdjskd",
                    Timestamp = 123,
                    TTL = 321,
                    Value = new byte[] {3, 2, 1}
                };
            var expectedAquilesColumn = new AquilesColumn
                {
                    ColumnName = StringHelpers.StringToBytes("djskdjskd"),
                    Timestamp = 123,
                    TTL = 321,
                    Value = new byte[] {3, 2, 1}
                };
            column.ToAquilesColumn().AssertEqualsTo(expectedAquilesColumn);
        }

        [Test]
        public void TestToColumn()
        {
            var aquilesColumn = new AquilesColumn
                {
                    ColumnName = StringHelpers.StringToBytes("qxx"),
                    Timestamp = 123,
                    TTL = 321,
                    Value = new byte[] {3, 2, 1}
                };
            var expectedColumn = new Column
                {
                    Name = "qxx",
                    Timestamp = 123,
                    TTL = 321,
                    Value = new byte[] {3, 2, 1}
                };
            aquilesColumn.ToColumn().AssertEqualsTo(expectedColumn);
        }
    }
}