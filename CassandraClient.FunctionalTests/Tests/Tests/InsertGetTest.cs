using System;
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
        public void TestDefaultTimeToLive()
        {
            columnFamilyConnectionDefaultTtl.AddColumn("row", new Column
                {
                    Name = "columnName",
                    Value = Encoding.UTF8.GetBytes("columnValue")
                });
            Thread.Sleep(10000);
            CheckNotFound("row", "columnName", columnFamilyConnectionDefaultTtl);
        }
        
        [Test]
        public void TestTimeToLiveIsMoreImportantThanDefaultTimeToLive()
        {
            columnFamilyConnectionDefaultTtl.AddColumn("row", new Column
                {
                    Name = "columnName",
                    Value = Encoding.UTF8.GetBytes("columnValue"),
                    TTL = 30,
                    Timestamp = 10
                });
            Thread.Sleep(10000);
            Check("row", "columnName", "columnValue", ttl: 30, cfc: columnFamilyConnectionDefaultTtl);
            Thread.Sleep(45000);
            CheckNotFound("row", "columnName", columnFamilyConnectionDefaultTtl);
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

        [Test]
        public void TestTimeToLiveIsProlongedWhenColumnIsUpdated()
        {
            const int ttl = 4;
            var rowKey = Guid.NewGuid().ToString();
            for(long timestamp = 0; timestamp < 10; timestamp++)
            {
                var columnValue = timestamp.ToString();
                columnFamilyConnection.AddColumn(rowKey, new Column
                    {
                        Name = "columnName",
                        Value = Encoding.UTF8.GetBytes(columnValue),
                        Timestamp = timestamp,
                        TTL = ttl,
                    });
                Thread.Sleep(TimeSpan.FromSeconds(ttl / 2.0));
                Check(rowKey, "columnName", columnValue, timestamp, ttl);
            }
            Thread.Sleep(TimeSpan.FromSeconds(ttl + 1));
            CheckNotFound(rowKey, "columnName");
        }
    }
}