﻿using System;
using System.Threading;

using NUnit.Framework;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class DescribeVersionTest : CassandraFunctionalTestBase
    {
        [Test, Ignore("Тест, который можно запустить ручками, во время выполнения вырубить кассандру и убедиться, что DescribeVersion делает сетевой вызов")]
        public void TestDescribeVersion()
        {
            var clusterConnection = cassandraCluster.RetrieveClusterConnection();
            for (var i = 0; i < 100; i++)
            {
                Console.WriteLine(i + " Version: " + clusterConnection.DescribeVersion());
                Thread.Sleep(1000);
            }
        }
    }
}