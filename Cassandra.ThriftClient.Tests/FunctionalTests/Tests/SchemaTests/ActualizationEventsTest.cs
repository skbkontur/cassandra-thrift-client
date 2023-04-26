using System.Linq;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Clusters.ActualizationEventListener;
using SkbKontur.Cassandra.ThriftClient.Schema;
using SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests.Utils;
using SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Tests.SchemaTests
{
    [TestFixture]
    public class ActualizationEventsTest
    {
        [SetUp]
        public void SetUp()
        {
            cluster = new CassandraCluster(LocalCassandraNodeExtensions.CreateSettings(), Logger.Instance);
            cassandraActualizerEventListener = new CassandraActualizerEventListener();
            cassandraSchemaActualizer = new CassandraSchemaActualizer(cluster, cassandraActualizerEventListener, Logger.Instance);
        }

        [TearDown]
        public void TearDown()
        {
            cluster.Dispose();
        }

        [Test]
        public void TestDoubleActualizeWithoutChangingSchema()
        {
            var scheme = new KeyspaceSchema
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
            cassandraSchemaActualizer.ActualizeKeyspaces(new[] {scheme}, changeExistingKeyspaceMetadata : true);
            Assert.That(cassandraActualizerEventListener.KeyspaceAddedInvokeCount, Is.EqualTo(1));
            Assert.That(cassandraActualizerEventListener.ColumnFamilyAddedInvokeCount, Is.EqualTo(0));
            Assert.That(cassandraActualizerEventListener.ColumnFamilyUpdatedInvokeCount, Is.EqualTo(0));
        }

        [Test]
        public void TestUpdateColumnFamilyProperty()
        {
            var scheme = new KeyspaceSchema
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
            scheme.Configuration.ColumnFamilies[0].Compression = ColumnFamilyCompression.Deflate(new CompressionOptions {ChunkLengthInKb = 1024});
            ActualizeKeyspaces(scheme);
            Assert.That(cassandraActualizerEventListener.ColumnFamilyUpdatedInvokeCount, Is.EqualTo(1));
        }

        [Test]
        public void TestAddNewColumnFamily()
        {
            var scheme = new KeyspaceSchema
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
            scheme.Configuration.ColumnFamilies = scheme.Configuration.ColumnFamilies.Concat(new[]
                {
                    new ColumnFamily
                        {
                            Name = "CF2"
                        }
                }).ToArray();
            ActualizeKeyspaces(scheme);
            Assert.That(cassandraActualizerEventListener.ColumnFamilyUpdatedInvokeCount, Is.EqualTo(0));
            Assert.That(cassandraActualizerEventListener.ColumnFamilyAddedInvokeCount, Is.EqualTo(1));
        }

        private void ActualizeKeyspaces(KeyspaceSchema schema)
        {
            cassandraSchemaActualizer.ActualizeKeyspaces(new[] {schema}, changeExistingKeyspaceMetadata : true);
        }

        private CassandraCluster cluster;
        private ICassandraSchemaActualizer cassandraSchemaActualizer;
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