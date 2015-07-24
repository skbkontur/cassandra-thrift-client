using System.Net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.ClusterDeployment;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class TestColumnFamilySchemaModificationCassandra
    {
        [SetUp]
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

            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];
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
        public void TestUpdateColumnFamily()
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(new CompactionStrategyOptions {SstableSizeInMb = 10}),
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

            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];

            Assert.That(columnFamily.Name, Is.EqualTo(originalColumnFamily.Name));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyType, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyType));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyOptions.SstableSizeInMb, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyOptions.SstableSizeInMb));
            Assert.That(columnFamily.GCGraceSeconds, Is.EqualTo(originalColumnFamily.GCGraceSeconds));
            Assert.That(columnFamily.ReadRepairChance, Is.EqualTo(originalColumnFamily.ReadRepairChance));

            originalColumnFamily.CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(5, 17);
            keyspaceConnection.UpdateColumnFamily(originalColumnFamily);

            columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];
            Assert.That(columnFamily.Name, Is.EqualTo(originalColumnFamily.Name));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyType, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyType));
            Assert.That(columnFamily.CompactionStrategy.MinCompactionThreshold, Is.EqualTo(originalColumnFamily.CompactionStrategy.MinCompactionThreshold));
            Assert.That(columnFamily.CompactionStrategy.MaxCompactionThreshold, Is.EqualTo(originalColumnFamily.CompactionStrategy.MaxCompactionThreshold));
            Assert.That(columnFamily.GCGraceSeconds, Is.EqualTo(originalColumnFamily.GCGraceSeconds));
            Assert.That(columnFamily.ReadRepairChance, Is.EqualTo(originalColumnFamily.ReadRepairChance));
        }

        [Test]
        public void TestCreateColumnFamilyWithCompression()
        {
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024, CrcCheckChance = 0.2}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 1024, CrcCheckChance = 0.2}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 0.2}));

            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 0.000002}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 0.000000000002}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2, CrcCheckChance = 1}));

            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.None());
        }

        [Test]
        public void TestTryCreateColumnFamilyWithWrongChunkLength()
        {
            Assert.Throws<CassandraClientInvalidRequestException>(
                () =>
                keyspaceConnection.AddColumnFamily(new ColumnFamily
                    {
                        Name = TestSchemaUtils.GetRandomColumnFamilyName(),
                        Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 3, CrcCheckChance = 1})
                    }));
        }

        [Test]
        public void TestDefaultCompression()
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    Compression = null
                };
            keyspaceConnection.AddColumnFamily(originalColumnFamily);

            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];
            Assert.That(columnFamily.Compression.Algorithm, Is.EqualTo(CompressionAlgorithms.Snappy));
            Assert.That(columnFamily.Compression.Options.ChunkLengthInKb, Is.Null);
            Assert.That(columnFamily.Compression.Options.CrcCheckChance, Is.Null);
        }

        private void InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression compression)
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    Compression = compression
                };
            keyspaceConnection.AddColumnFamily(originalColumnFamily);

            var columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];
            Assert.That(columnFamily.Compression.Algorithm, Is.EqualTo(originalColumnFamily.Compression.Algorithm));
            if(originalColumnFamily.Compression.Options != null)
            {
                Assert.That(columnFamily.Compression.Options.ChunkLengthInKb, Is.EqualTo(originalColumnFamily.Compression.Options.ChunkLengthInKb));
                Assert.That(columnFamily.Compression.Options.CrcCheckChance, Is.EqualTo(originalColumnFamily.Compression.Options.CrcCheckChance));
            }
            else
                Assert.That(columnFamily.Compression.Options, Is.Null);
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
            if(columnFamilyCaching != null)
                Assert.That(columnFamily.Caching, Is.EqualTo(columnFamilyCaching));
            else
                Assert.That(columnFamily.Caching, Is.EqualTo(ColumnFamilyCaching.KeysOnly));
        }

        private CassandraNode node;
        private CassandraCluster cluster;
        private IKeyspaceConnection keyspaceConnection;
    }
}