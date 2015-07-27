using System.Text;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class InsertGetTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestAddGet()
        {
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue")
                });
            Check("someRow", "someColumnName", "someColumnValue");
        }

        [Test]
        public void TestDeleteAdd()
        {
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue")
                });

            Check("someRow", "someColumnName", "someColumnValue");
            columnFamilyConnection.DeleteBatch("someRow", new[] {"someColumnName"});
            CheckNotFound("someRow", "someColumnName");
            Thread.Sleep(1);
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue")
                });
            Check("someRow", "someColumnName", "someColumnValue");
        }

        [Test]
        public void TestDoubleAdd()
        {
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue1")
                });
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue2")
                });
            Check("someRow", "someColumnName", "someColumnValue2");
        }

        [Test]
        public void TestAddDelete()
        {
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue1")
                });
            columnFamilyConnection.DeleteBatch("someRow", new[] {"someColumnName"});
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestDoubleDelete()
        {
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue1")
                });
            columnFamilyConnection.DeleteBatch("someRow", new[] {"someColumnName"});
            columnFamilyConnection.DeleteBatch("someRow", new[] {"someColumnName"});
            CheckNotFound("someRow", "someColumnName");
        }

        [Test]
        public void TestDeleteNotExistingColumn()
        {
            columnFamilyConnection.DeleteBatch("someRow", new[] {"someColumnName"});
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
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue1"),
                    Timestamp = 10
                });
            columnFamilyConnection.AddColumn("someRow", new Column
                {
                    Name = "someColumnName",
                    Value = Encoding.UTF8.GetBytes("someColumnValue2"),
                    Timestamp = 9
                });
            Check("someRow", "someColumnName", "someColumnValue1", 10);
        }

        [Test]
        public void TestTimeToLive()
        {
            columnFamilyConnection.AddColumn("row", new Column
                {
                    Name = "columnName",
                    Value = Encoding.UTF8.GetBytes("columnValue"),
                    TTL = 1
                });
            Thread.Sleep(10000);
            CheckNotFound("row", "columnName");
        }

        [Test]
        public void TestTimeToLiveNotDependsOnTimestamp()
        {
            columnFamilyConnection.AddColumn("row", new Column
                {
                    Name = "columnName",
                    Value = Encoding.UTF8.GetBytes("columnValue"),
                    Timestamp = 0,
                    TTL = 30
                });
            Thread.Sleep(15000);
            Check("row", "columnName", "columnValue", 0, 30);
            Thread.Sleep(45000);
            CheckNotFound("row", "columnName");
        }
    }
}