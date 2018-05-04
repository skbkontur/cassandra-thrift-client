using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;
using SKBKontur.Cassandra.FunctionalTests.Utils.ObjComparer;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class UpdateKeyspaceTest : CassandraFunctionalTestBase
    {
        [TestCase(true)]
        [TestCase(false)]
        public void TestUpdateKeyspace(bool durableWrites)
        {
            var keyspaceName = Guid.NewGuid().ToString("N");
            cassandraCluster.ActualizeKeyspaces(new[]
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
                                                },
                                        }
                                }
                        }
                });

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
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[] {new DataCenterReplicationFactor("dc1", 3),}),
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

            if(expected.ColumnFamilies == null)
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
                            ColumnFamilies = new[] {new ColumnFamily
                                {
                                    Name = columnFamilyName,
                                    BloomFilterFpChance = null,
                                }},
                        }
                };
            cassandraCluster.ActualizeKeyspaces(new[] {keyspaceScheme});
            var actualColumnFamily = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().First(x => x.Name == keyspaceName).ColumnFamilies[columnFamilyName];
            Assert.AreEqual(0.01d, actualColumnFamily.BloomFilterFpChance);

            keyspaceScheme.Configuration.ColumnFamilies.First().BloomFilterFpChance = 0.02;
            cassandraCluster.ActualizeKeyspaces(new[] {keyspaceScheme});

            actualColumnFamily = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().First(x => x.Name == keyspaceName).ColumnFamilies[columnFamilyName];
            Assert.AreEqual(0.02d, actualColumnFamily.BloomFilterFpChance);
        }
    }
}