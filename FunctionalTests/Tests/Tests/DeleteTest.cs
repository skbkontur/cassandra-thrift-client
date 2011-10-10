using System;
using System.Threading;

using CassandraClient.Abstractions;

using NUnit.Framework;

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
                conn.DeleteBatch("qxx", new[] {"qzz", "qtt", "qqq"}, 10000);
                var columns = conn.GetRow("qxx", null, 20);
                Assert.AreEqual(0, columns.Length);
                var keys = conn.GetKeys("", 100);
                Assert.AreEqual(1, keys.Length);
            }
        }
    }
}