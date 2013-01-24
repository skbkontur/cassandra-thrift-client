using System.Linq;
using System.Text;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class BatchCommandsTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void TestDeleteBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            columnFamilyConnection.AddBatch("someKey", columns);
            columnFamilyConnection.DeleteBatch("someKey", columns.Select(column => column.Name));
            foreach(var column in columns)
                CheckNotFound("someKey", column.Name);
        }

        [Test]
        public void TestDeleteBatchWithSmallTimestampIsNotDelete()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            columnFamilyConnection.AddBatch("someKey", columns);
            columnFamilyConnection.DeleteBatch("someKey", columns.Select(column => column.Name), 123);
            foreach(var column in columns)
                Check("someKey", column.Name, ToString(column.Value));
        }

        [Test]
        public void TestDoubleDeleteBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            columnFamilyConnection.AddBatch("someKey", columns);
            columnFamilyConnection.DeleteBatch("someKey", columns.Select(column => column.Name));
            columnFamilyConnection.DeleteBatch("someKey", columns.Select(column => column.Name));
            foreach(var column in columns)
                CheckNotFound("someKey", column.Name);
        }

        [Test]
        public void TestPermanentDelete()
        {
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue")
                });
            Check("someRow", "someColumnName", "someColumnValue");
            columnFamilyConnection.DeleteBatch("someRow", new[] {"someColumnName"}, long.MaxValue);
            CheckNotFound("someRow", "someColumnName");
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue")
                });
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestAddBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            columnFamilyConnection.AddBatch("someKey", columns);
            foreach(var column in columns)
                Check("someKey", column.Name, ToString(column.Value));
        }
    }
}