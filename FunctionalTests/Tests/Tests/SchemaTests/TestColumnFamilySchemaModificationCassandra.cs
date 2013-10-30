using System.Linq;
using System.Net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.FunctionalTests.Management;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class TestColumnFamilySchemaModificationCassandra
    {
        [TestFixtureSetUp]
        public void SetUp()
        {
            node = StartSingleCassandraSetUp.Node;
            cluster = new CassandraCluster(node.CreateSettings(IPAddress.Loopback));

            var clusterConnection = cluster.RetrieveClusterConnection();
            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                    ReplicationFactor = 1
                };
            clusterConnection.AddKeyspace(createdKeyspace);

            keyspaceConnection = cluster.RetrieveKeyspaceConnection(keyspaceName);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
        }

        [Test]
        public void TestCreateColumnFamily()
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            keyspaceConnection.AddColumnFamily(new ColumnFamily
                {
                    Name = name,
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions { SstableSizeInMb = 10 }),
                    GCGraceSeconds = 123,
                    ReadRepairChance = 0.3
                });

            var columnFamilies = keyspaceConnection.DescribeKeyspace().ColumnFamilies.ToList();
            Assert.That(columnFamilies.Count, Is.EqualTo(1));
        }

        private CassandraNode node;
        private CassandraCluster cluster;
        private IKeyspaceConnection keyspaceConnection;
    }
}