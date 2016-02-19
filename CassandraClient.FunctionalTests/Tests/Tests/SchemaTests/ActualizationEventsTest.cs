using System.Linq;
using System.Net;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Clusters.ActualizationEventListener;
using SKBKontur.Cassandra.CassandraClient.Scheme;
using SKBKontur.Cassandra.ClusterDeployment;
using SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Utils;

namespace SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class ActualizationEventsTest
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraCluster(StartSingleCassandraSetUp.Node.CreateSettings(IPAddress.Loopback));
            cassandraActualizerEventListener = new CassandraActualizerEventListener();
            actualizer = new SchemeActualizer(cluster, cassandraActualizerEventListener);
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
            Assert.That(cassandraActualizerEventListener.KeyspaceAddedInvokeCount, Is.EqualTo(0));
            actualizer.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cassandraActualizerEventListener.KeyspaceAddedInvokeCount, Is.EqualTo(1));
            Assert.That(cassandraActualizerEventListener.ColumnFamilyAddedInvokeCount, Is.EqualTo(0));
            Assert.That(cassandraActualizerEventListener.ColumnFamilyUpdatedInvokeCount, Is.EqualTo(0));
        }

        [Test]
        public void TestUpdateColumnFamilyProperty()
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
            actualizer.ActualizeKeyspaces(new[] {scheme});
            scheme.Configuration.ColumnFamilies[0].Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024});
            actualizer.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cassandraActualizerEventListener.ColumnFamilyUpdatedInvokeCount, Is.EqualTo(1));
        }

        [Test]
        public void TestAddNewColumnFamily()
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
            actualizer.ActualizeKeyspaces(new[] {scheme});
            scheme.Configuration.ColumnFamilies = scheme.Configuration.ColumnFamilies.Concat(new[]
                {
                    new ColumnFamily
                        {
                            Name = "CF2"
                        }
                }).ToArray();
            actualizer.ActualizeKeyspaces(new[] {scheme});
            Assert.That(cassandraActualizerEventListener.ColumnFamilyUpdatedInvokeCount, Is.EqualTo(0));
            Assert.That(cassandraActualizerEventListener.ColumnFamilyAddedInvokeCount, Is.EqualTo(1));
        }

        private CassandraCluster cluster;
        private SchemeActualizer actualizer;
        private CassandraActualizerEventListener cassandraActualizerEventListener;

        private class CassandraActualizerEventListener : ICassandraActualizerEventListener
        {
            public void ActualizationStarted()
            {
            }

            public void SchemaRetrieved(Keyspace[] keyspaces)
            {
            }

            public void KeyspaceActualizationStarted(string keyspaceName)
            {
            }

            public void KeyspaceAdded(Keyspace keyspace)
            {
                KeyspaceAddedInvokeCount++;
            }

            public void ActualizationCompleted()
            {
            }

            public void ColumnFamilyUpdated(string keyspaceName, ColumnFamily columnFamily)
            {
                ColumnFamilyUpdatedInvokeCount++;
            }

            public void ColumnFamilyAdded(string keyspaceName, ColumnFamily columnFamily)
            {
                ColumnFamilyAddedInvokeCount++;
            }

            public int ColumnFamilyUpdatedInvokeCount { get; private set; }
            public int ColumnFamilyAddedInvokeCount { get; private set; }
            public int KeyspaceAddedInvokeCount { get; private set; }
        }
    }
}