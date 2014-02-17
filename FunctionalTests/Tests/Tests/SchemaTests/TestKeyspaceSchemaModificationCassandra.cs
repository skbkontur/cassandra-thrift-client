using System.Linq;
using System.Net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.ClusterDeployment;

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
            var clusterConnection = cluster.RetrieveClusterConnection();

            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                    ReplicationFactor = 1
                };
            clusterConnection.AddKeyspace(createdKeyspace);

            var keyspaces = clusterConnection.RetrieveKeyspaces();
            var actualKeyspace = keyspaces.First(x => x.Name == keyspaceName);
            AssertKeyspacePropertiesEquals(createdKeyspace, actualKeyspace);
        }

        [Test]
        public void TestUpdateKeyspaceReplicationFactor()
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace {Name = keyspaceName, ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy", ReplicationFactor = 1};
            clusterConnection.AddKeyspace(createdKeyspace);
            createdKeyspace.ReplicationFactor = 2;
            clusterConnection.UpdateKeyspace(createdKeyspace);

            var keyspaces = clusterConnection.RetrieveKeyspaces();
            var actualKeyspace = keyspaces.First(x => x.Name == keyspaceName);
            AssertKeyspacePropertiesEquals(createdKeyspace, actualKeyspace);
        }

        [Test]
        public void TestTryAddKeyspaceWithInvalidName()
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var createdKeyspace = new Keyspace {Name = "Keyspace-123-123", ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy", ReplicationFactor = 1};
            Assert.Throws<CassandraClientInvalidRequestException>(() => clusterConnection.AddKeyspace(createdKeyspace));
        }

        [Test]
        public void TestTryAddKeyspaceWithInvalidReplicaPlacementStrategy()
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var createdKeyspace = new Keyspace { Name = TestSchemaUtils.GetRandomKeyspaceName(), ReplicaPlacementStrategy = "InvalidStrategy", ReplicationFactor = 1 };
            Assert.Throws<CassandraClientInvalidRequestException>(() => clusterConnection.AddKeyspace(createdKeyspace));
        }

        private void AssertKeyspacePropertiesEquals(Keyspace createdKeyspace, Keyspace actualKeyspace)
        {
            Assert.That(actualKeyspace.Name, Is.EqualTo(createdKeyspace.Name));
            Assert.That(actualKeyspace.ReplicaPlacementStrategy, Is.EqualTo(createdKeyspace.ReplicaPlacementStrategy));
            Assert.That(actualKeyspace.ReplicationFactor, Is.EqualTo(createdKeyspace.ReplicationFactor));
        }

        private CassandraNode node;
        private CassandraCluster cluster;
    }
}