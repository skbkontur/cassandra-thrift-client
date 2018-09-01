using System.Linq;

using Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Utils;
using Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class TestKeyspaceSchemaModificationCassandra
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraCluster(SingleCassandraNodeSetUpFixture.Node.CreateSettings(), Logger.Instance);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
        }

        [TestCase(true)]
        [TestCase(false)]
        public void TestAddKeyspace(bool durableWrites)
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    DurableWrites = durableWrites,
                    ReplicationStrategy = SimpleReplicationStrategy.Create(1)
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
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    ReplicationStrategy = SimpleReplicationStrategy.Create(1)
                };

            clusterConnection.AddKeyspace(createdKeyspace);
            createdKeyspace.ReplicationStrategy = SimpleReplicationStrategy.Create(2);
            clusterConnection.UpdateKeyspace(createdKeyspace);

            var keyspaces = clusterConnection.RetrieveKeyspaces();
            var actualKeyspace = keyspaces.First(x => x.Name == keyspaceName);
            AssertKeyspacePropertiesEquals(createdKeyspace, actualKeyspace);
        }

        [Test]
        public void TestTryAddKeyspaceWithInvalidName()
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var createdKeyspace = new Keyspace
                {
                    Name = "Keyspace-123-123",
                    ReplicationStrategy = SimpleReplicationStrategy.Create(1)
                };
            Assert.Throws<CassandraClientInvalidRequestException>(() => clusterConnection.AddKeyspace(createdKeyspace));
        }

        [Test]
        public void TestTryAddKeyspaceWithMultiDcConfiguration()
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[]
                        {
                            new DataCenterReplicationFactor("dc1", 3),
                            new DataCenterReplicationFactor("dc2", 5)
                        })
                };
            clusterConnection.AddKeyspace(createdKeyspace);

            var keyspaces = clusterConnection.RetrieveKeyspaces();
            var actualKeyspace = keyspaces.First(x => x.Name == keyspaceName);
            AssertKeyspacePropertiesEquals(createdKeyspace, actualKeyspace);
        }

        [Test]
        public void TestUpdateKeyspaceWithMultiDcConfiguration()
        {
            var clusterConnection = cluster.RetrieveClusterConnection();

            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[]
                        {
                            new DataCenterReplicationFactor("dc1", 3),
                            new DataCenterReplicationFactor("dc2", 5)
                        })
                };
            clusterConnection.AddKeyspace(createdKeyspace);

            createdKeyspace.ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[]
                {
                    new DataCenterReplicationFactor("dc3", 7)
                });
            clusterConnection.UpdateKeyspace(createdKeyspace);

            var keyspaces = clusterConnection.RetrieveKeyspaces();
            var actualKeyspace = keyspaces.First(x => x.Name == keyspaceName);
            AssertKeyspacePropertiesEquals(createdKeyspace, actualKeyspace);
        }

        private void AssertKeyspacePropertiesEquals(Keyspace createdKeyspace, Keyspace actualKeyspace)
        {
            Assert.That(actualKeyspace.Name, Is.EqualTo(createdKeyspace.Name));
            Assert.That(actualKeyspace.DurableWrites, Is.EqualTo(createdKeyspace.DurableWrites));
            Assert.AreEqual(createdKeyspace.ReplicationStrategy.Name, actualKeyspace.ReplicationStrategy.Name);
            Assert.AreEqual(createdKeyspace.ReplicationStrategy.StrategyOptions, actualKeyspace.ReplicationStrategy.StrategyOptions);
        }

        private CassandraCluster cluster;
    }
}