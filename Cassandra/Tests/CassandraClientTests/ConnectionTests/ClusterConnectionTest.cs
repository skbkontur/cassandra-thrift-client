using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Aquiles;
using Aquiles.Command;
using Aquiles.Command.System;
using Aquiles.Model;

using CassandraClient.Abstractions;
using CassandraClient.Connections;
using CassandraClient.Helpers;

using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;

using Rhino.Mocks;

namespace Cassandra.Tests.CassandraClientTests.ConnectionTests
{
    public class ClusterConnectionTest : TestBase
    {
        #region Setup/Teardown

        public override void SetUp()
        {
            base.SetUp();
            aquilesConnection = GetMock<IAquilesConnection>();
            clusterConnection = new ClusterConnection(aquilesConnection, ConsistencyLevel.ALL,
                                                      ConsistencyLevel.EACH_QUORUM);
        }

        #endregion

        [Test]
        public void TestAddKeyspace()
        {
            var keyspace = new Keyspace
                {
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"a", new ColumnFamily {Keyspace = "q", Name = "t"}}
                        },
                    Name = "name",
                    ReplicaPlacementStrategy = "strategy",
                    ReplicationFactor = 4556
                };

            aquilesConnection.Expect(connection => connection.Execute(ARG.EqualsTo(new AddKeyspaceCommand
                {
                    ConsistencyLevel =
                        AquilesConsistencyLevel.
                        EACH_QUORUM,
                    KeyspaceDefinition =
                        keyspace.
                        ToAquilesKeyspace()
                })));
            aquilesConnection.Expect(connection => connection.Execute(Arg<SchemaAgreementCommand>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((SchemaAgreementCommand)invocation.Arguments[0])
                );
            clusterConnection.AddKeyspace(keyspace);
        }

        [Test]
        public void TestRemoveKeyspace()
        {
            aquilesConnection.Expect(connection => connection.Execute(ARG.EqualsTo(new DropKeyspaceCommand
                {
                    ConsistencyLevel =
                        AquilesConsistencyLevel.
                        EACH_QUORUM,
                    Keyspace = "keyspace"
                })));
            aquilesConnection.Expect(connection => connection.Execute(Arg<SchemaAgreementCommand>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((SchemaAgreementCommand)invocation.Arguments[0])
                );
            clusterConnection.RemoveKeyspace("keyspace");
        }

        [Test]
        public void TestRetrieveKeyspacesWhenNoKeyspaces()
        {
            var command = new RetrieveKeyspacesCommand
                {
                    ConsistencyLevel = AquilesConsistencyLevel.ALL
                };
            aquilesConnection
                .Expect(connection => connection.Execute(ARG.EqualsTo(command)))
                .WhenCalled(
                    invocation =>
                    SetKeyspaces((RetrieveKeyspacesCommand)invocation.Arguments[0], new List<AquilesKeyspace>()));

            CollectionAssert.IsEmpty(clusterConnection.RetrieveKeyspaces());
        }

        [Test]
        public void TestRetrieveKeyspacesWhenNullKeyspaces()
        {
            var command = new RetrieveKeyspacesCommand
                {
                    ConsistencyLevel = AquilesConsistencyLevel.ALL
                };
            aquilesConnection
                .Expect(connection => connection.Execute(ARG.EqualsTo(command)))
                .WhenCalled(invocation => SetKeyspaces((RetrieveKeyspacesCommand)invocation.Arguments[0], null));

            CollectionAssert.IsEmpty(clusterConnection.RetrieveKeyspaces());
        }

        [Test]
        public void TestRetrieveKeyspaces()
        {
            var command = new RetrieveKeyspacesCommand
                {
                    ConsistencyLevel = AquilesConsistencyLevel.ALL
                };

            var keyspaces = new List<AquilesKeyspace>(new[]
                {
                    new AquilesKeyspace
                        {
                            Name = "testName",
                            ReplicationFactor = 34232,
                            ReplicationPlacementStrategy = "strategy",
                            ColumnFamilies = new Dictionary<string, AquilesColumnFamily>
                                {
                                    {"a", new AquilesColumnFamily {Name = "b", Keyspace = "c"}},
                                    {"d", new AquilesColumnFamily {Name = "e", Keyspace = "f"}}
                                }
                        }, new AquilesKeyspace
                            {
                                Name = "system",
                                ReplicationFactor = 1322,
                                ReplicationPlacementStrategy = "systemStrategy"
                            },
                    new AquilesKeyspace
                        {
                            Name = "qxx"
                        }
                });
            aquilesConnection
                .Expect(connection => connection.Execute(ARG.EqualsTo(command)))
                .WhenCalled(invocation => SetKeyspaces((RetrieveKeyspacesCommand)invocation.Arguments[0], keyspaces));

            var expectedResult = new List<Keyspace>(new[]
                {
                    new Keyspace
                        {
                            Name = "testName",
                            ReplicationFactor = 34232,
                            ColumnFamilies = new Dictionary<string, ColumnFamily>
                                {
                                    {"a", new ColumnFamily {Name = "b", Keyspace = "c"}},
                                    {"d", new ColumnFamily {Name = "e", Keyspace = "f"}}
                                },
                            ReplicaPlacementStrategy = "strategy"
                        }, new Keyspace
                            {
                                Name = "qxx",
                                ReplicaPlacementStrategy = "org.apache.cassandra.locator.SimpleStrategy"
                            }
                });

            var retrieveKeyspaces = clusterConnection.RetrieveKeyspaces();

            var actualResult = retrieveKeyspaces.ToArray();
            actualResult.AssertEqualsTo(expectedResult.ToArray());
        }

        [Test]
        public void TestAddColumnFamily()
        {
            const string keyspace = "keyspace";
            const string columnFamilyName = "familyName";
            aquilesConnection.Expect(c => c.Execute(Arg<AddColumnFamilyCommand>.Is.TypeOf)).WhenCalled(
                i =>
                    {
                        var addColumnFamilyCommand = (AddColumnFamilyCommand)i.Arguments[0];
                        Assert.That(addColumnFamilyCommand.ColumnFamilyDefinition.Name, Is.EqualTo(columnFamilyName));
                        Assert.That(addColumnFamilyCommand.ColumnFamilyDefinition.Keyspace, Is.EqualTo(keyspace));
                        Assert.That(addColumnFamilyCommand.ConsistencyLevel, Is.EqualTo(AquilesConsistencyLevel.EACH_QUORUM));
                    });
            aquilesConnection.Expect(connection => connection.Execute(Arg<SchemaAgreementCommand>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((SchemaAgreementCommand)invocation.Arguments[0])
                );
            clusterConnection.AddColumnFamily(keyspace, columnFamilyName);
        }

        [Test]
        public void TestDescribeKeyspace()
        {
            const string keyspaceName = "keyspace";
            const int replicationFactor = 5;
            const string replicationPlacementStrategy = "ReplicationPlacementStrategy";
            aquilesConnection.Expect(c => c.Execute(Arg<DescribeKeyspaceCommand>.Is.TypeOf)).WhenCalled(
                i =>
                    {
                        var describeKeyspaceCommand = (DescribeKeyspaceCommand)i.Arguments[0];
                        var aquilesKeyspace = new AquilesKeyspace
                            {
                                ColumnFamilies = new Dictionary<string, AquilesColumnFamily>
                                    {
                                        {"CFName1", new AquilesColumnFamily {Keyspace = keyspaceName, Name = "CFName1"}},
                                        {"CFName2", new AquilesColumnFamily {Keyspace = keyspaceName, Name = "CFName2"}}
                                    },
                                Name = keyspaceName,
                                ReplicationFactor = replicationFactor,
                                ReplicationPlacementStrategy = replicationPlacementStrategy
                            };

                        Type describeKeyspaceCommandType = typeof(DescribeKeyspaceCommand);
                        PropertyInfo propertyInfo = describeKeyspaceCommandType.GetProperty("KeyspaceInformation");
                        propertyInfo.SetValue(describeKeyspaceCommand, aquilesKeyspace, null);
                    });

            Keyspace keyspace = clusterConnection.DescribeKeyspace(keyspaceName);
            Assert.That(keyspace.ColumnFamilies.Count, Is.EqualTo(2));
            Assert.That(keyspace.ColumnFamilies["CFName1"].Name, Is.EqualTo("CFName1"));
            Assert.That(keyspace.ColumnFamilies["CFName1"].Keyspace, Is.EqualTo(keyspaceName));

            Assert.That(keyspace.ColumnFamilies["CFName2"].Name, Is.EqualTo("CFName2"));
            Assert.That(keyspace.ColumnFamilies["CFName2"].Keyspace, Is.EqualTo(keyspaceName));

            Assert.That(keyspace.Name, Is.EqualTo(keyspaceName));
            Assert.That(keyspace.ReplicaPlacementStrategy, Is.EqualTo(replicationPlacementStrategy));
            Assert.That(keyspace.ReplicationFactor, Is.EqualTo(replicationFactor));
        }

        private static void SetKeyspaces(RetrieveKeyspacesCommand command, List<AquilesKeyspace> keyspaces)
        {
            Type retrieveKeyspaceCommandType = typeof(RetrieveKeyspacesCommand);
            PropertyInfo propertyInfo = retrieveKeyspaceCommandType.GetProperty("Keyspaces");
            propertyInfo.SetValue(command, keyspaces, null);
        }

        private static object SetOutput(SchemaAgreementCommand command)
        {
            var setMethod = typeof(SchemaAgreementCommand).GetProperty("Output").GetSetMethod(true);
            return setMethod.Invoke(command, new[] {new Dictionary<string, List<string>> {{"zzz", null}}});
        }

        private IAquilesConnection aquilesConnection;
        private ClusterConnection clusterConnection;
    }
}