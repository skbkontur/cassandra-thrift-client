using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class ConnectionTimeoutTest : CassandraFunctionalTestBase
    {
        [Test]
        public void Test()
        {
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            conn.AddColumn("qxx", new Column {Name = "qzz", Value = new byte[] {1, 2, 3}});
            Thread.Sleep(20000);
            conn.AddColumn("qxx", new Column {Name = "qyy", Value = new byte[] {1, 2, 3}});
        }
    }
}