﻿using System.Threading;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests
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