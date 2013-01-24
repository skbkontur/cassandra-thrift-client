using System.Linq;

using NUnit.Framework;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class BatchCommandsTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void TestDeleteBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            cassandraClient.AddBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns);
            cassandraClient.DeleteBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns.Select(column => column.Name));
            foreach(var column in columns)
                CheckNotFound("someKey", column.Name);
        }

        [Test]
        public void TestDeleteBatchWithSmallTimestampIsNotDelete()
        {
            var columns = new[] { ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f") };
            cassandraClient.AddBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns);
            cassandraClient.DeleteBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns.Select(column => column.Name), 123);
            foreach (var column in columns)
                Check("someKey", column.Name, ToString(column.Value));
        }

        [Test]
        public void TestDoubleDeleteBatch()
        {
            var columns = new[] { ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f") };
            cassandraClient.AddBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns);
            cassandraClient.DeleteBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns.Select(column => column.Name));
            cassandraClient.DeleteBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns.Select(column => column.Name));
            foreach (var column in columns)
                CheckNotFound("someKey", column.Name);
        }

        [Test]
        public void TestPermanentDelete()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue");
            Check("someRow", "someColumnName", "someColumnValue");
            cassandraClient.DeleteBatch(KeyspaceName, Constants.ColumnFamilyName,
                "someRow", new[] { "someColumnName" }, long.MaxValue);
            CheckNotFound("someRow", "someColumnName");
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue");
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestAddBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            cassandraClient.AddBatch(KeyspaceName, Constants.ColumnFamilyName, "someKey", columns);
            foreach(var column in columns)
                Check("someKey", column.Name, ToString(column.Value));
        }
    }
}