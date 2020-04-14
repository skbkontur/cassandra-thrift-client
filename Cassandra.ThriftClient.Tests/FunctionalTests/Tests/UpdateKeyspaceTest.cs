﻿using System;
using System.Collections.Generic;
using System.Linq;

using Cassandra.ThriftClient.Tests.FunctionalTests.Utils.ObjComparer;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Scheme;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Tests
{
    public class UpdateKeyspaceTest : CassandraFunctionalTestBase
    {
        [TestCase(true)]
        [TestCase(false)]
        public void TestUpdateKeyspace(bool durableWrites)
        {
            var keyspaceName = Guid.NewGuid().ToString("N");
            cassandraSchemaActualizer.ActualizeKeyspaces(new[]
                {
                    new KeyspaceScheme
                        {
                            Name = keyspaceName,
                            Configuration = new KeyspaceConfiguration
                                {
                                    DurableWrites = durableWrites,
                                    ReplicationStrategy = SimpleReplicationStrategy.Create(1),
                                    ColumnFamilies = new[]
                                        {
                                            new ColumnFamily
                                                {
                                                    Name = "1"
                                                },
                                            new ColumnFamily
                                                {
                                                    Name = "2"
                                                },
                                            new ColumnFamily
                                                {
                                                    Name = "3"
                                                }
                                        }
                                }
                        }
                }, changeExistingKeyspaceMetadata : false);

            var keyspaces = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray();
            AssertKeyspacesEquals(new Keyspace
                {
                    Name = keyspaceName,
                    DurableWrites = durableWrites,
                    ReplicationStrategy = SimpleReplicationStrategy.Create(1),
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"1", new ColumnFamily {Name = "1"}},
                            {"2", new ColumnFamily {Name = "2"}},
                            {"3", new ColumnFamily {Name = "3"}}
                        }
                }, keyspaces.Single(keyspace1 => keyspace1.Name == keyspaceName));

            cassandraCluster.RetrieveClusterConnection().UpdateKeyspace(new Keyspace
                {
                    Name = keyspaceName,
                    DurableWrites = durableWrites,
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[] {new DataCenterReplicationFactor("dc1", 3)})
                });

            var keyspace = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray().Single(keyspace1 => keyspace1.Name == keyspaceName);

            AssertKeyspacesEquals(new Keyspace
                {
                    Name = keyspaceName,
                    DurableWrites = durableWrites,
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[] {new DataCenterReplicationFactor("dc1", 3)}),
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"1", new ColumnFamily {Name = "1"}},
                            {"2", new ColumnFamily {Name = "2"}},
                            {"3", new ColumnFamily {Name = "3"}}
                        }
                }, keyspace);
        }

        private static void AssertKeyspacesEquals(Keyspace expected, Keyspace actual)
        {
            Assert.AreEqual(expected.Name, actual.Name);
            Assert.AreEqual(expected.DurableWrites, actual.DurableWrites);
            Assert.AreEqual(expected.ReplicationStrategy.Name, actual.ReplicationStrategy.Name);
            Assert.AreEqual(expected.ReplicationStrategy.StrategyOptions, actual.ReplicationStrategy.StrategyOptions);

            if (expected.ColumnFamilies == null)
                Assert.IsNull(actual.ColumnFamilies);
            else
            {
                Assert.NotNull(actual.ColumnFamilies);
                actual.ColumnFamilies.Keys.OrderByDescending(s => s).ToArray().AssertEqualsTo(expected.ColumnFamilies.Keys.OrderByDescending(s => s).ToArray());
            }
        }

        [Test]
        public void TestActualizeColumnFamily()
        {
            var keyspaceName = Guid.NewGuid().ToString("N");
            const string columnFamilyName = "1";
            var keyspaceScheme = new KeyspaceScheme
                {
                    Name = keyspaceName,
                    Configuration = new KeyspaceConfiguration
                        {
                            ReplicationStrategy = SimpleReplicationStrategy.Create(1),
                            ColumnFamilies = new[]
                                {
                                    new ColumnFamily
                                        {
                                            Name = columnFamilyName,
                                            BloomFilterFpChance = null
                                        }
                                }
                        }
                };
            cassandraSchemaActualizer.ActualizeKeyspaces(new[] {keyspaceScheme}, changeExistingKeyspaceMetadata : false);
            var actualColumnFamily = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().First(x => x.Name == keyspaceName).ColumnFamilies[columnFamilyName];
            Assert.AreEqual(0.01d, actualColumnFamily.BloomFilterFpChance);

            keyspaceScheme.Configuration.ColumnFamilies.First().BloomFilterFpChance = 0.02;
            cassandraSchemaActualizer.ActualizeKeyspaces(new[] {keyspaceScheme}, changeExistingKeyspaceMetadata : false);

            actualColumnFamily = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().First(x => x.Name == keyspaceName).ColumnFamilies[columnFamilyName];
            Assert.AreEqual(0.02d, actualColumnFamily.BloomFilterFpChance);
        }
    }
}
