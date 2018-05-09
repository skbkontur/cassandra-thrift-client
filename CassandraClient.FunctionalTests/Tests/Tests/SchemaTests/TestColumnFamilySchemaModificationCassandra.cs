using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Connections;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Utils;
using SKBKontur.Cassandra.FunctionalTests.Utils;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class TestColumnFamilySchemaModificationCassandra
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraCluster(SingleCassandraNodeSetUpFixture.Node.CreateSettings(), new Log4NetWrapper(typeof(TestColumnFamilySchemaModificationCassandra)));

            var clusterConnection = cluster.RetrieveClusterConnection();
            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var createdKeyspace = new Keyspace
                {
                    Name = keyspaceName,
                    ReplicationStrategy = SimpleReplicationStrategy.Create(1)
                };
            clusterConnection.AddKeyspace(createdKeyspace);

            keyspaceConnection = cluster.RetrieveKeyspaceConnection(keyspaceName);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
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
                Assert.That(columnFamily.Compression.Options.ChunkLengthInKb, Is.EqualTo(originalColumnFamily.Compression.Options.ChunkLengthInKb));
            else
                Assert.That(columnFamily.Compression.Options, Is.Null);
        }

        private void InternalTestCaching(ColumnFamilyCaching? columnFamilyCaching)
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(sstableSizeInMb : 10),
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

        private CassandraCluster cluster;
        private IKeyspaceConnection keyspaceConnection;

        [Test]
        public void TestCreateColumnFamily()
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(sstableSizeInMb : 10),
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
        public void TestCreateColumnFamilyWithCompression()
        {
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.LZ4(new CompressionOptions {ChunkLengthInKb = 128}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.LZ4(new CompressionOptions {ChunkLengthInKb = 1024}));

            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 32}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Snappy(new CompressionOptions {ChunkLengthInKb = 1024}));

            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 2}));
            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024}));

            InternalTestCreateColumnFamilyCompression(ColumnFamilyCompression.None());
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
            Assert.That(columnFamily.Compression.IsEnabled, Is.True);
            Assert.That(columnFamily.Compression.Algorithm, Is.EqualTo(CompressionAlgorithms.LZ4));
            Assert.That(columnFamily.Compression.Options.ChunkLengthInKb, Is.Null.Or.EqualTo(64));
        }

        [Test]
        public void TestTryCreateColumnFamilyWithWrongChunkLength()
        {
            Assert.Throws<CassandraClientInvalidRequestException>(
                () =>
                    keyspaceConnection.AddColumnFamily(new ColumnFamily
                        {
                            Name = TestSchemaUtils.GetRandomColumnFamilyName(),
                            Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 3})
                        }));
        }

        [Test]
        public void TestUpdateColumnFamily()
        {
            var name = TestSchemaUtils.GetRandomColumnFamilyName();
            var originalColumnFamily = new ColumnFamily
                {
                    Name = name,
                    CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(sstableSizeInMb : 10),
                    GCGraceSeconds = 123,
                    ReadRepairChance = 0.3,
                    Caching = ColumnFamilyCaching.All
                };
            keyspaceConnection.AddColumnFamily(originalColumnFamily);

            originalColumnFamily.CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(sstableSizeInMb : 20);
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

            originalColumnFamily.CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(minThreshold : 5, maxThreshold : 17);
            keyspaceConnection.UpdateColumnFamily(originalColumnFamily);

            columnFamily = keyspaceConnection.DescribeKeyspace().ColumnFamilies[name];
            Assert.That(columnFamily.Name, Is.EqualTo(originalColumnFamily.Name));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyType, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyType));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyOptions.Enabled, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyOptions.Enabled));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyOptions.MinThreshold, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyOptions.MinThreshold));
            Assert.That(columnFamily.CompactionStrategy.CompactionStrategyOptions.MaxThreshold, Is.EqualTo(originalColumnFamily.CompactionStrategy.CompactionStrategyOptions.MaxThreshold));
            Assert.That(columnFamily.GCGraceSeconds, Is.EqualTo(originalColumnFamily.GCGraceSeconds));
            Assert.That(columnFamily.ReadRepairChance, Is.EqualTo(originalColumnFamily.ReadRepairChance));
        }
    }
}