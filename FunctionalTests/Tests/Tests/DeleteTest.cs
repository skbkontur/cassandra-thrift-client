using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class DeleteTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test, Description("После удаления row его id остается до compaction'а")]
        public void RangeGhostsTest()
        {
            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                conn.AddColumn("qxx", new Column
                {
                    Name = "qzz",
                    Timestamp = 10,
                    Value = new byte[] { 2, 3, 4 }
                });
                conn.AddColumn("qxx", new Column
                {
                    Name = "qtt",
                    Timestamp = 10,
                    Value = new byte[] { 2, 3, 4 }
                });
                conn.AddColumn("qxx", new Column
                {
                    Name = "qqq",
                    Timestamp = 10,
                    Value = new byte[] { 2, 3, 4 }
                });
                conn.DeleteRow("qxx", 10000);
                var columns = conn.GetRow("qxx", null, 20);
                Assert.AreEqual(0, columns.Length);
                var keys = conn.GetKeys("", 100);
                Assert.AreEqual(1, keys.Length);
            }
        }

        [Test, Description("После удаления всех колонок из row сам row также удаляется после compaction'а")]
        public void RangeGhostsTestVersion2()
        {
            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                conn.AddColumn("qxx", new Column
                {
                    Name = "qzz",
                    Timestamp = 10,
                    Value = new byte[] { 2, 3, 4 }
                });
                conn.AddColumn("qxx", new Column
                {
                    Name = "qtt",
                    Timestamp = 10,
                    Value = new byte[] { 2, 3, 4 }
                });
                conn.AddColumn("qxx", new Column
                {
                    Name = "qqq",
                    Timestamp = 10,
                    Value = new byte[] { 2, 3, 4 }
                });
                conn.DeleteBatch("qxx", new[] { "qzz", "qtt", "qqq" }, 10000);
                var columns = conn.GetRow("qxx", null, 20);
                Assert.AreEqual(0, columns.Length);
                var keys = conn.GetKeys("", 100);
                Assert.AreEqual(1, keys.Length);
            }
        }

        [Test]
        public void DeleteRowsSimpleTest()
        {
            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                var rowKeys = new List<string> { "1", "2", "3", "4", "5" };
                foreach (var rowKey in rowKeys)
                {
                    conn.AddColumn(rowKey, new Column
                    {
                        Name = "qzz" + rowKey,
                        Timestamp = 10,
                        Value = new byte[] { 2, 3, 4 }
                    });
                    conn.AddColumn(rowKey, new Column
                    {
                        Name = "qtt" + rowKey,
                        Timestamp = 10,
                        Value = new byte[] { 2, 3, 4 }
                    });
                }
                conn.DeleteRows(rowKeys.ToArray(), 20);
                foreach (var rowKey in rowKeys)
                {
                    Assert.AreEqual(0, conn.GetRow(rowKey).ToArray().Length);
                }
            }
        }

        [Test]
        public void DeleteRowsHardTest()
        {
            const int rowCount = 100;
            const int minColumnCount = 200;
            const int maxColumnCount = 300;
            const int batchSize = 50;
            var random = new Random();
            var columnCount = new int[rowCount];
            for (int i = 0; i < rowCount; i++)
            {
                columnCount[i] = random.Next(minColumnCount, maxColumnCount);
            }

            const int addTimestamp = 1;
            var keys = new List<string>();
            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                for (int i = 0; i < rowCount; i++)
                {
                    var forAdd = new List<KeyValuePair<string, IEnumerable<Column>>>();
                    Console.WriteLine("Writing " + i * 100 / rowCount + " percents");
                    var key = i.ToString();
                    keys.Add(key);
                    var columns = new List<Column>();
                    for (int j = 0; j < columnCount[i]; j++)
                        columns.Add(new Column
                            {
                                Name = j.ToString(),
                                Timestamp = addTimestamp,
                                Value = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 }
                            });
                    forAdd.Add(new KeyValuePair<string, IEnumerable<Column>>(key, columns));
                    conn.BatchInsert(forAdd);
                }
            }

            const int deleteTimestamp = 1;
            Console.WriteLine("Deleting");
            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                conn.DeleteRows(keys.ToArray(), deleteTimestamp, batchSize);
            }

            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                for (int i = 0; i < keys.Count; i++)
                {
                    var key = keys[i];
                    Console.WriteLine("Checking " + i * 100 / rowCount + " percents");
                    Assert.AreEqual(0, conn.GetRow(key).ToArray().Length);
                }
            }
        }
    }
}