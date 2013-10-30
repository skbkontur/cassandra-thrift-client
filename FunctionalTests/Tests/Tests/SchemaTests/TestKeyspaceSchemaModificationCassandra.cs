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
    public class TestKeyspaceSchemaModificationCassandra
    {
        [SetUp]
        public void SetUp()
        {
            node = StartSingleCassandraSetUp.Node;
            cluster = new CassandraCluster(node.CreateSettings(IPAddress.Loopback));
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
        }

        [Test]
        public void TestAddKeyspace()
        {
            using(var clusterConnection = cluster.RetrieveClusterConnection())
            {
                var keyspaceName = Guid.NewGuid().ToString();
                clusterConnection.AddKeyspace(new Keyspace
                {
                    Name = keyspaceName,
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                    ReplicationFactor = 1
                });
                var keyspaces = clusterConnection.RetrieveKeyspaces();
                Assert.That(keyspaces.Any(x => x.Name == keyspaceName));                
            }
        }

        private CassandraNode node;
        private CassandraCluster cluster;
    }
}