using System;
using System.IO;
using System.Linq;
using System.Net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.FunctionalTests.Management;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class TestCassandra12
    {
        [SetUp]
        public void SetUp()
        {
            node = StartSingleCassandraSetUp.Node;
        }

        [Test]
        public void TestAddKeyspace()
        {
            var cluster = new CassandraCluster(node.CreateSettings(IPAddress.Loopback));
            var clusterConnection = cluster.RetrieveClusterConnection();
            clusterConnection.AddKeyspace(new Keyspace
                {
                    Name = "Test_123", 
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy", 
                    ReplicationFactor = 1
                });
            var keyspaces = clusterConnection.RetrieveKeyspaces();
            Assert.That(keyspaces.Any(x => x.Name == "Test_123"));
        }

        private CassandraNode node;
    }
}