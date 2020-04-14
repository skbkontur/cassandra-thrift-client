using Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Spies;
using Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Utils;
using Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Scheme;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class ActualizeKeyspaceTest
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraClusterSpy(() => new CassandraCluster(SingleCassandraNodeSetUpFixture.Node.CreateSettings(), Logger.Instance));
            cassandraSchemaActualizer = new CassandraSchemaActualizer(cluster, null, Logger.Instance);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
        }

        private void ActualizeKeyspaces(KeyspaceScheme scheme)
        {
            cassandraSchemaActualizer.ActualizeKeyspaces(new[] {scheme}, changeExistingKeyspaceMetadata : true);
        }

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

            cassandraSchemaActualizer.ActualizeKeyspaces(keyspaceSchemes, changeExistingKeyspaceMetadata : false);

            originalColumnFamily.Caching = ColumnFamilyCaching.None;
            cassandraSchemaActualizer.ActualizeKeyspaces(keyspaceSchemes, changeExistingKeyspaceMetadata : false);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.None));

            originalColumnFamily.Caching = ColumnFamilyCaching.KeysOnly;
            cassandraSchemaActualizer.ActualizeKeyspaces(keyspaceSchemes, changeExistingKeyspaceMetadata : false);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.KeysOnly));

            originalColumnFamily.Caching = ColumnFamilyCaching.RowsOnly;
            cassandraSchemaActualizer.ActualizeKeyspaces(keyspaceSchemes, changeExistingKeyspaceMetadata : false);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.RowsOnly));

            originalColumnFamily.Caching = ColumnFamilyCaching.All;
            cassandraSchemaActualizer.ActualizeKeyspaces(keyspaceSchemes, changeExistingKeyspaceMetadata : false);
            Assert.That(cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace().ColumnFamilies[name].Caching, Is.EqualTo(ColumnFamilyCaching.All));
        }

        private CassandraClusterSpy cluster;
        private ICassandraSchemaActualizer cassandraSchemaActualizer;
    }
}
