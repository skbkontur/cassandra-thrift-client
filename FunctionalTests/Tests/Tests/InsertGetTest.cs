using System.Threading;

using NUnit.Framework;

namespace Tests.Tests
{
    public class InsertGetTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestAddGet()
        {
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue");
            Check("someRow", "someColumnName", "someColumnValue");
        }

        [Test]
        public void TestDoubleAdd()
        {
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue1");
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue2");
            Check("someRow", "someColumnName", "someColumnValue2");
        }

        [Test]
        public void TestNotFound()
        {
            CheckNotFound("qxx", "zzz");
        }

        [Test]
        public void TestSmallerTimestampInSecondAdd()
        {
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue1", 10);
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "someRow", "someColumnName", "someColumnValue2", 9);
            Check("someRow", "someColumnName", "someColumnValue1", 10);
        }

        [Test]
        public void TestTimeToLive()
        {
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", "columnName", "columnValue", ttl : 1);
            Thread.Sleep(2000);
            CheckNotFound("row", "columnName");
        }

        [Test]
        public void TestTimeToLiveNotDependsOnTimestamp()
        {
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", "columnName", "columnValue", 0, 1);
            Thread.Sleep(500);
            Check("row", "columnName", "columnValue", 0, 1);
            Thread.Sleep(2000);
            CheckNotFound("row", "columnName");
        }
    }
}