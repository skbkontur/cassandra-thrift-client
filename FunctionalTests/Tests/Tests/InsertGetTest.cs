using System.Threading;

using NUnit.Framework;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class InsertGetTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void TestAddGet()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue");
            Check("someRow", "someColumnName", "someColumnValue");
        }

        [Test]
        public void TestDeleteAdd()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue");
            Check("someRow", "someColumnName", "someColumnValue");
            columnFamilyConnection.DeleteBatch("someRow", new []{"someColumnName"});
            CheckNotFound("someRow", "someColumnName");
            Thread.Sleep(1);
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue");
            Check("someRow", "someColumnName", "someColumnValue");
        }
        

        [Test]
        public void TestDoubleAdd()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue1");
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue2");
            Check("someRow", "someColumnName", "someColumnValue2");
        }

        [Test]
        public void TestAddDelete()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue1");
            columnFamilyConnection.DeleteBatch("someRow", new []{"someColumnName"});
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestDoubleDelete()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue1");
            columnFamilyConnection.DeleteBatch("someRow", new []{"someColumnName"});
            columnFamilyConnection.DeleteBatch("someRow", new []{"someColumnName"});
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestDeleteNotExistingColumn()
        {
            columnFamilyConnection.DeleteBatch("someRow", new[] { "someColumnName" });
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestNotFound()
        {
            CheckNotFound("qxx", "zzz");
        }

        [Test]
        public void TestSmallerTimestampInSecondAdd()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue1", 10);
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue2", 9);
            Check("someRow", "someColumnName", "someColumnValue1", 10);
        }

        [Test]
        public void TestTimeToLive()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "row", "columnName", "columnValue", ttl : 1);
            Thread.Sleep(10000);
            CheckNotFound("row", "columnName");
        }

        [Test]
        public void TestTimeToLiveNotDependsOnTimestamp()
        {
            cassandraClient.Add(KeyspaceName, Constants.ColumnFamilyName, "row", "columnName", "columnValue", 0, 30);
            Thread.Sleep(15000);
            Check("row", "columnName", "columnValue", 0, 30);
            Thread.Sleep(45000);
            CheckNotFound("row", "columnName");
        }
    }
}