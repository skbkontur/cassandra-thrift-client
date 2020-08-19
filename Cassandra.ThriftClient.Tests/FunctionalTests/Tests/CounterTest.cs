using System;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class CounterTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestGetCount()
        {
            var start1 = Timestamp.Now;
            const int rowsCount = 100;
            const int columnsCount = 100;
            for (var i = 0; i < rowsCount; i++)
            {
                var cols = new Column[columnsCount];
                for (var j = 0; j < columnsCount; ++j)
                {
                    var columnName = "columnName" + i + "_" + j;
                    var columnValue = "columnValue" + i + "_" + j;
                    cols[j] = ToColumn(columnName, columnValue, 100);
                }

                columnFamilyConnection.AddBatch("row", cols);
            }
            var finish1 = Timestamp.Now;
            var start = Timestamp.Now;
            var count = columnFamilyConnection.GetCount("row");
            var finish = Timestamp.Now;
            Assert.AreEqual(rowsCount * columnsCount, count);
            Console.WriteLine("GetCount Completed at " + (finish - start).TotalMilliseconds + "ms");
            Console.WriteLine("Write Completed at " + (finish1 - start1).TotalMilliseconds + "ms");
        }

        [Test]
        public void TestGetEmptyCount()
        {
            var count = columnFamilyConnection.GetCount("roww");
            Assert.AreEqual(0, count);
            count = columnFamilyConnection.GetCount("row");
            Assert.AreEqual(0, count);
        }

        [Test]
        public void TestGetCounts()
        {
            var rows = new string[10];
            for (var i = 0; i < rows.Length; i++)
            {
                var cols = new Column[10 * i];
                for (var j = 0; j < 10 * i; ++j)
                {
                    var columnName = "columnName" + i + "_" + j;
                    var columnValue = "columnValue" + i + "_" + j;
                    cols[j] = ToColumn(columnName, columnValue, 100);
                }
                rows[i] = "row" + i;
                columnFamilyConnection.AddBatch("row" + i, cols);
            }
            var counts = columnFamilyConnection.GetCounts(rows);
            for (var i = 0; i < 10; ++i)
            {
                Assert.AreEqual(10 * i, counts["row" + i]);
            }
        }
    }
}