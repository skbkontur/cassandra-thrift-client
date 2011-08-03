using System.Collections.Generic;
using System.Linq;

using Aquiles.Model;

using CassandraClient.Abstractions;
using CassandraClient.Helpers;

using NUnit.Framework;

namespace Cassandra.Tests.CassandraClientTests.HelpersTests
{
    public class KeyspaceConverterTest : TestBase
    {
        [Test]
        public void TestNull()
        {
            Assert.IsNull(((Keyspace)null).ToAquilesKeyspace());
            Assert.IsNull(((AquilesKeyspace)null).ToKeyspace());
        }

        [Test]
        public void TestToAquilesKeyspace()
        {
            var keyspace = new Keyspace
                {
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"a", new ColumnFamily {Name = "q"}},
                            {"b", new ColumnFamily {Name = "r"}}
                        },
                    Name = "name",
                    ReplicationFactor = 56,
                    ReplicaPlacementStrategy = "strategy"
                };

            var expectedAquilesKeyspace = new AquilesKeyspace
                {
                    ColumnFamilies = new Dictionary<string, AquilesColumnFamily>
                        {
                            {
                                "a",
                                new AquilesColumnFamily
                                    {Name = "q", Keyspace = "name", Comparator = "UTF8Type"}
                                },
                            {
                                "b",
                                new AquilesColumnFamily
                                    {Name = "r", Keyspace = "name", Comparator = "UTF8Type"}
                                }
                        },
                    Name = "name",
                    ReplicationFactor = 56,
                    ReplicationPlacementStrategy = "strategy"
                };

            AquilesKeyspace actualAquilesKeypace = keyspace.ToAquilesKeyspace();
            actualAquilesKeypace.ColumnFamilies.ToArray().AssertEqualsTo(
                expectedAquilesKeyspace.ColumnFamilies.ToArray());
            expectedAquilesKeyspace.ColumnFamilies = actualAquilesKeypace.ColumnFamilies = null;
            actualAquilesKeypace.AssertEqualsTo(expectedAquilesKeyspace);
        }

        [Test]
        public void TestToKeyspace()
        {
            var aquilesKeyspace = new AquilesKeyspace
                {
                    ColumnFamilies = new Dictionary<string, AquilesColumnFamily>
                        {
                            {
                                "a",
                                new AquilesColumnFamily
                                    {Name = "q", Keyspace = "z"}
                                },
                            {
                                "b",
                                new AquilesColumnFamily
                                    {Name = "r", Keyspace = "t"}
                                }
                        },
                    Name = "name",
                    ReplicationFactor = 56,
                    ReplicationPlacementStrategy = "strategy",
                    ReplicationPlacementStrategyOptions = new Dictionary<string, string>
                        {
                            {"key", "value"}
                        }
                };
            var expectedKeyspace = new Keyspace
                {
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"a", new ColumnFamily { Name = "q"}},
                            {"b", new ColumnFamily { Name = "r"}}
                        },
                    Name = "name",
                    ReplicationFactor = 56,
                    ReplicaPlacementStrategy = "strategy"
                };

            Keyspace actualKeypace = aquilesKeyspace.ToKeyspace();
            actualKeypace.ColumnFamilies.ToArray().AssertEqualsTo(expectedKeyspace.ColumnFamilies.ToArray());
            expectedKeyspace.ColumnFamilies = actualKeypace.ColumnFamilies = null;
            actualKeypace.AssertEqualsTo(expectedKeyspace);
        }
    }
}