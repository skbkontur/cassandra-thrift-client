using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class IsRowExistTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void SimpleTest()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            Assert.IsFalse(conn.IsRowExist("trashId"));
            conn.AddBatch("id1", new[]
                {
                    new Column
                        {
                            Name = "qzz",
                            Timestamp = 1,
                            Value = new byte[] {1, 2, 3}
                        }
                });

            conn.AddBatch("id3", new[]
                {
                    new Column
                        {
                            Name = "qzz",
                            Timestamp = 1,
                            Value = new byte[] {1, 2, 3}
                        },
                    new Column
                        {
                            Name = "qxx",
                            Timestamp = 2,
                            Value = new byte[] {1, 2, 3}
                        }
                });
            Assert.IsTrue(conn.IsRowExist("id1"));
            Assert.IsFalse(conn.IsRowExist("id2"));
            Assert.IsTrue(conn.IsRowExist("id3"));
        }

        [Test]
        public void TestTryGetColumn()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            Column column;
            
            Assert.IsFalse(conn.TryGetColumn("id1", "qzz", out column));
            Assert.IsFalse(conn.TryGetColumn("id1", "qxx", out column));
            Assert.IsFalse(conn.TryGetColumn("id2", "qzz", out column));
            Assert.IsFalse(conn.TryGetColumn("id2", "qxx", out column));
            Assert.IsFalse(conn.TryGetColumn("id3", "qzz", out column));
            Assert.IsFalse(conn.TryGetColumn("id3", "qxx", out column));

            conn.AddBatch("id1", new[]
                {
                    new Column
                        {
                            Name = "qzz",
                            Timestamp = 1,
                            Value = new byte[] {1}
                        }
                });

            conn.AddBatch("id3", new[]
                {
                    new Column
                        {
                            Name = "qzz",
                            Timestamp = 1,
                            Value = new byte[] {2}
                        },
                    new Column
                        {
                            Name = "qxx",
                            Timestamp = 2,
                            Value = new byte[] {3}
                        }
                });
            
            Assert.IsTrue(conn.TryGetColumn("id1", "qzz", out column));
            CollectionAssert.AreEqual(new byte[]{1}, column.Value);
            Assert.IsFalse(conn.TryGetColumn("id1", "qxx", out column));
            Assert.IsFalse(conn.TryGetColumn("id2", "qzz", out column));
            Assert.IsFalse(conn.TryGetColumn("id2", "qxx", out column));
            Assert.IsTrue(conn.TryGetColumn("id3", "qzz", out column));
            CollectionAssert.AreEqual(new byte[] { 2 }, column.Value);
            Assert.IsTrue(conn.TryGetColumn("id3", "qxx", out column));
            CollectionAssert.AreEqual(new byte[] { 3 }, column.Value);
        }
    }
}