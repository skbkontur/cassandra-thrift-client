using System.Net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Scheme;
using SKBKontur.Cassandra.FunctionalTests.Management;
using SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Spies;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class ActualizeKeyspaceTest
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraClusterSpy(() => new CassandraCluster(StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback)));
            actualize = new SchemeActualizer(cluster);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
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
            actualize.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));
            actualize.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));
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
            actualize.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(0));
            scheme.Configuration.ColumnFamilies[0].Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024});
            actualize.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cluster.UpdateColumnFamilyInvokeCount, Is.EqualTo(1));
        }

        [Test]
        public void TestActualizeWithNullProperties()
        {
            string keyspaceName = TestSchemaUtils.GetRandomKeyspaceName();
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
                                            Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {  ChunkLengthInKb = 1024 })
                                        }
                                }
                        }
                };
            actualize.ActualizeKeyspaces(new[] {scheme});
            
            var actualScheme = cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace();
            Assert.That(actualScheme.ColumnFamilies["CF1"].Compression.Algorithm, Is.EqualTo(CompressionAlgorithm.Deflate));

            scheme.Configuration.ColumnFamilies[0].Compression = null;
            scheme.Configuration.ColumnFamilies[0].Caching = ColumnFamilyCaching.All;
            actualize.ActualizeKeyspaces(new[] {scheme});

            actualScheme = cluster.RetrieveKeyspaceConnection(keyspaceName).DescribeKeyspace();
            Assert.That(actualScheme.ColumnFamilies["CF1"].Compression.Algorithm, Is.EqualTo(CompressionAlgorithm.Deflate));
        }

        private CassandraClusterSpy cluster;
        private SchemeActualizer actualize;
    }
}