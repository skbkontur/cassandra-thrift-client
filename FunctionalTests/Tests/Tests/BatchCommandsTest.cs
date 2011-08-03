using System.Linq;

using NUnit.Framework;

namespace Tests.Tests
{
    public class BatchCommandsTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void TestDeleteBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            cassandraClient.AddBatch(Constants.KeyspaceName, Constants.ColumnFamilyName, "someKey", columns);
            cassandraClient.DeleteBatch(Constants.KeyspaceName, Constants.ColumnFamilyName, "someKey", columns.Select(column => column.Name));
            foreach(var column in columns)
                CheckNotFound("someKey", column.Name);
        }

        [Test]
        public void TestAddBatch()
        {
            var columns = new[] {ToColumn("a", "b"), ToColumn("c", "d"), ToColumn("e", "f")};
            cassandraClient.AddBatch(Constants.KeyspaceName, Constants.ColumnFamilyName, "someKey", columns);
            foreach(var column in columns)
                Check("someKey", column.Name, ToString(column.Value));
        }
    }
}