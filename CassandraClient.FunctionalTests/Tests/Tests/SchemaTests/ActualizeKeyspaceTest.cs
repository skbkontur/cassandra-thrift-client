using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Scheme;
using SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Spies;
using SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Utils;
using SKBKontur.Cassandra.FunctionalTests.Utils;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class ActualizeKeyspaceTest
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraClusterSpy(() => new CassandraCluster(SingleCassandraNodeSetUpFixture.Node.CreateSettings(), Logger.Instance));
            actualize = new SchemeActualizer(cluster, null, Logger.Instance);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
        }

        private void ActualizeKeyspaces(KeyspaceScheme scheme)
        {
            actualize.ActualizeKeyspaces(new[] {scheme}, changeExistingKeyspaceMetadata : true);
        }

        private CassandraClusterSpy cluster;
        private SchemeActualizer actualize;

        [Test]
        public void TestActualizeWithNullProperties()
        {
            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var scheme = new KeyspaceScheme
                {
                    Name = keyspaceName,
                    Configuration = new KeyspaceConfiguration
                        {
                            ColumnFamilies = new[]
                                {
                                    new ColumnFamily
                                        {
                                            Name = "CF1",
                                            Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024}),
                                            Caching = ColumnFamilyCaching.KeysOnly
                                        }
                                }
                        }
                };
            ActualizeKeyspaces(scheme);

            var actualScheme = cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace();
            Assert.That(actualScheme.ColumnFamilies["CF1"].Compression.Algorithm, Is.EqualTo(CompressionAlgorithms.Deflate));

            scheme.Configuration.ColumnFamilies[0].Compression = null;
            scheme.Configuration.ColumnFamilies[0].Caching = ColumnFamilyCaching.All;
            ActualizeKeyspaces(scheme);

            actualScheme = cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace();
            Assert.That(actualScheme.ColumnFamilies["CF1"].Compression.Algorithm, Is.EqualTo(CompressionAlgorithms.LZ4));
        }

        [Test]
        public void TestChangeCompactionProperties()
        {
            var scheme = new KeyspaceScheme
                {
                    Name = TestSchemaUtils.GetRandomKeyspaceName(),
                    Configuration = new KeyspaceConfiguration
                        {
                            ColumnFamilies = new[]
                                {
                                    new ColumnFamily
                                        {
                                            Name = "CF1"
                                        }
                                }
                        }
                };
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));

            scheme.Configuration.ColumnFamilies[0].CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(4, 32);
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));

            scheme.Configuration.ColumnFamilies[0].CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(3, 32);
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(1));

            scheme.Configuration.ColumnFamilies[0].CompactionStrategy = CompactionStrategy.SizeTieredCompactionStrategy(3, 31);
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(2));

            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(2));

            scheme.Configuration.ColumnFamilies[0].CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(10);
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(3));

            scheme.Configuration.ColumnFamilies[0].CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(11);
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(4));

            scheme.Configuration.ColumnFamilies[0].CompactionStrategy = CompactionStrategy.LeveledCompactionStrategy(11);
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(4));
        }

        [Test]
        public void TestChangeCompressionProperty()
        {
            var scheme = new KeyspaceScheme
                {
                    Name = TestSchemaUtils.GetRandomKeyspaceName(),
                    Configuration = new KeyspaceConfiguration
                        {
                            ColumnFamilies = new[]
                                {
                                    new ColumnFamily
                                        {
                                            Name = "CF1"
                                        }
                                }
                        }
                };
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));
            scheme.Configuration.ColumnFamilies[0].Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024});
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(1));
        }

        [Test]
        public void TestDoubleActualizeWithoutChangingSchema()
        {
            var scheme = new KeyspaceScheme
                {
                    Name = TestSchemaUtils.GetRandomKeyspaceName(),
                    Configuration = new KeyspaceConfiguration
                        {
                            ColumnFamilies = new[]
                                {
                                    new ColumnFamily
                                        {
                                            Name = "CF1"
                                        }
                                }
                        }
                };
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));
            ActualizeKeyspaces(scheme);
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));
        }

        [Test]
        public void TestUpdateColumnFamilyCaching()
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
            var keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
            var keyspaceSchemes = new[]
                {
                    new KeyspaceScheme
                        {
                            Name = keyspaceName,
                            Configuration = new KeyspaceConfiguration
                                {
                                    ColumnFamilies = new[]
                                        {
                                            originalColumnFamily
                                        }
                                }
                        }
                };

            cluster.ActualizeKeyspaces(keyspaceSchemes);

            originalColumnFamily.Caching = ColumnFamilyCaching.None;
            cluster.ActualizeKeyspaces(keyspaceSchemes);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.None));

            originalColumnFamily.Caching = ColumnFamilyCaching.KeysOnly;
            cluster.ActualizeKeyspaces(keyspaceSchemes);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.KeysOnly));

            originalColumnFamily.Caching = ColumnFamilyCaching.RowsOnly;
            cluster.ActualizeKeyspaces(keyspaceSchemes);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.RowsOnly));

            originalColumnFamily.Caching = ColumnFamilyCaching.All;
            cluster.ActualizeKeyspaces(keyspaceSchemes);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.All));
        }
    }
}