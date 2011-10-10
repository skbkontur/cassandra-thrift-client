using System.Threading;

using CassandraClient.Abstractions;

using NUnit.Framework;

namespace Tests.Tests
{
    public class ConnectionTimeoutTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void Test()
        {
            using (var conn = cassandraCluster.RetrieveColumnFamilyConnection(Constants.KeyspaceName, Constants.ColumnFamilyName))
            {
                conn.AddColumn("qxx", new Column{Name = "qzz", Value = new byte[]{1,2,3}});
                Thread.Sleep(20000);
                conn.AddColumn("qxx", new Column { Name = "qyy", Value = new byte[] { 1, 2, 3 } });
            }
        }
    }
}