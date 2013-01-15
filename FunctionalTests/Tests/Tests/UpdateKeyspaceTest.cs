using System.Collections.Generic;
using System.Linq;

using Cassandra.Tests;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class UpdateKeyspaceTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestUpdateKeyspace()
        {
            var cassandraClient = new CassandraClient(cassandraCluster);
            cassandraClient.RemoveAllKeyspaces();

            cassandraCluster.RetrieveClusterConnection().AddKeyspace(new Keyspace
                {
                    Name = "KeyspaceName",
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                    ReplicationFactor = 1,
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"1", new ColumnFamily {Name = "1"}},
                            {"2", new ColumnFamily {Name = "2"}},
                            {"3", new ColumnFamily {Name = "3"}}
                        }
                });

            var keyspaces = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray();
            Assert.AreEqual(1, keyspaces.Length);
            AssertKeyspacesEquals(new Keyspace
                {
                    Name = "KeyspaceName",
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy",
                    ReplicationFactor = 1,
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"1", new ColumnFamily {Name = "1"}},
                            {"2", new ColumnFamily {Name = "2"}},
                            {"3", new ColumnFamily {Name = "3"}}
                        }
                }, keyspaces[0]);

            cassandraCluster.RetrieveClusterConnection().UpdateKeyspace(new Keyspace
                {
                    Name = "KeyspaceName",
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.NetworkTopologyStrategy",
                    ReplicationFactor = 3
                });

            keyspaces = cassandraCluster.RetrieveClusterConnection().RetrieveKeyspaces().ToArray();
            Assert.AreEqual(1, keyspaces.Length);

            //The replication factor is zero, because for NetworkTopologyStrategy this setting does not work and should be adjusted 
            //per datacenter
            AssertKeyspacesEquals(new Keyspace
                {
                    Name = "KeyspaceName",
                    ReplicaPlacementStrategy = "org.apache.cassandra.locator.NetworkTopologyStrategy",
                    ReplicationFactor = 0,
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"1", new ColumnFamily {Name = "1"}},
                            {"2", new ColumnFamily {Name = "2"}},
                            {"3", new ColumnFamily {Name = "3"}}
                        }
                }, keyspaces[0]);
        }

        private void AssertKeyspacesEquals(Keyspace expected, Keyspace actual)
        {
            Assert.AreEqual(expected.Name, actual.Name);
            Assert.AreEqual(expected.ReplicaPlacementStrategy, actual.ReplicaPlacementStrategy);
            Assert.AreEqual(expected.ReplicationFactor, actual.ReplicationFactor);

            if(expected.ColumnFamilies == null)
                Assert.IsNull(actual.ColumnFamilies);
            else
            {
                Assert.NotNull(actual.ColumnFamilies);
                actual.ColumnFamilies.Keys.OrderByDescending(s => s).ToArray().AssertEqualsTo(expected.ColumnFamilies.Keys.OrderByDescending(s => s).ToArray());
            }
        }
    }
}