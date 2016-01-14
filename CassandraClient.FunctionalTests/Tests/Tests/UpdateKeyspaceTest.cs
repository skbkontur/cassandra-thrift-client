using System;
using System.Collections.Generic;
using System.Linq;

using Cassandra.Tests;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Scheme;
using SKBKontur.Cassandra.FunctionalTests.Tests.SchemaTests.Utils;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class UpdateKeyspaceTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestUpdateKeyspace()
        {
            var keyspaceName = Guid.NewGuid().ToString("N");
            cassandraCluster.ActualizeKeyspaces(new[]{new KeyspaceScheme
                {
                    Name = keyspaceName,
                    Configuration = new KeyspaceConfiguration
                        {
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
                }});

            var keyspaces = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray();
            AssertKeyspacesEquals(new Keyspace
                {
                    Name = keyspaceName,
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
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[]{new DataCenterReplicationFactor { DataCenterName = "dc1", ReplicationFactor = 3}, })
                });

            var keyspace = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray().Single(keyspace1 => keyspace1.Name == keyspaceName);

            AssertKeyspacesEquals(new Keyspace
                {
                    Name = keyspaceName,
                    ReplicationStrategy = NetworkTopologyReplicationStrategy.Create(new[]{new DataCenterReplicationFactor { DataCenterName = "dc1", ReplicationFactor = 3}, }),
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
                            ColumnFamilies = new[]{new ColumnFamily{Name = columnFamilyName}}
                        }
                };
            cassandraCluster.ActualizeKeyspaces(new[]{keyspaceScheme});
            var actualColumnFamily = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().First(x => x.Name == keyspaceName).ColumnFamilies[columnFamilyName];
            Assert.IsNull(actualColumnFamily.BloomFilterFpChance);

            keyspaceScheme.Configuration.ColumnFamilies.First().BloomFilterFpChance = 0.01;
            cassandraCluster.ActualizeKeyspaces(new[]{keyspaceScheme});

            actualColumnFamily = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().First(x => x.Name == keyspaceName).ColumnFamilies[columnFamilyName];
            Assert.AreEqual(0.01, actualColumnFamily.BloomFilterFpChance);

        }
    }
}