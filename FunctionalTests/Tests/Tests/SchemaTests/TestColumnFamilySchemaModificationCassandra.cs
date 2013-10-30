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
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 10}),
                    GCGraceSeconds = 123,
                    ReadRepairChance = 0.3
                };
            keyspaceConnection.AddColumnFamily(originalColumnFamily);

            var columnFamilies = keyspaceConnection.DescribeKeyspace().ColumnFamilies.ToList();
            Assert.That(columnFamilies.Count, Is.EqualTo(1));

            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies.First().Value;
            Assert.That(columnFamily.Name, Is.EqualTo(originalColumnFamily.Name));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyType, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyType));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyOptions.SstableSizeInMb, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyOptions.SstableSizeInMb));
            Assert.That(columnFamily.GCGraceSeconds, Is.EqualTo(originalColumnFamily.GCGraceSeconds));
            Assert.That(columnFamily.ReadRepairChance, Is.EqualTo(originalColumnFamily.ReadRepairChance));
        }

        [Test]
        public void TestCreateColumnFamilyWithKeyCache()
        {
            InternalTestCaching(null);
            InternalTestCaching(ColumnFamilyCaching.KeysOnly);
            InternalTestCaching(ColumnFamilyCaching.RowsOnly);
            InternalTestCaching(ColumnFamilyCaching.None);
            InternalTestCaching(ColumnFamilyCaching.All);
        }

        [Test]
        public void TestUpdateCreateColumnFamily()
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
            {
                Name = name,
                CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions { SstableSizeInMb = 10 }),
                GCGraceSeconds = 123,
                ReadRepairChance = 0.3,
                Caching = ColumnFamilyCaching.All
            };
            keyspaceConnection.AddColumnFamily(originalColumnFamily);

            originalColumnFamily.CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 20});
            originalColumnFamily.GCGraceSeconds = 321;
            originalColumnFamily.ReadRepairChance = 0.9;
            originalColumnFamily.Caching = ColumnFamilyCaching.None;
            keyspaceConnection.UpdateColumnFamily(originalColumnFamily);

            var columnFamilies = keyspaceConnection.DescribeKeyspace().ColumnFamilies.ToList();
            Assert.That(columnFamilies.Count, Is.EqualTo(1));

            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies.First().Value;
            Assert.That(columnFamily.Name, Is.EqualTo(originalColumnFamily.Name));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyType, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyType));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyOptions.SstableSizeInMb, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyOptions.SstableSizeInMb));
            Assert.That(columnFamily.GCGraceSeconds, Is.EqualTo(originalColumnFamily.GCGraceSeconds));
            Assert.That(columnFamily.ReadRepairChance, Is.EqualTo(originalColumnFamily.ReadRepairChance));
        }

        private void InternalTestCaching(ColumnFamilyCaching? columnFamilyCaching)
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name, 
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 10}), 
                    GCGraceSeconds = 123, 
                    ReadRepairChance = 0.3
                };
            if(columnFamilyCaching != null)
                originalColumnFamily.Caching = columnFamilyCaching.Value;
            keyspaceConnection.AddColumnFamily(originalColumnFamily);


            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];
            if (columnFamilyCaching != null)
                Assert.That(columnFamily.Caching, Is.EqualTo(columnFamilyCaching));
            else
                Assert.That(columnFamily.Caching, Is.EqualTo(ColumnFamilyCaching.KeysOnly));
        }

        private CassandraNode node;
        private CassandraCluster cluster;
        private IKeyspaceConnection keyspaceConnection;
    }
}