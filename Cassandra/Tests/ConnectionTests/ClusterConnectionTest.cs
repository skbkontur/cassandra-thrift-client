using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;



using CassandraClient.AquilesTrash.Model;

using CassandraClient.Abstractions;
using CassandraClient.AquilesTrash.Command;
using CassandraClient.AquilesTrash.Command.System;
using CassandraClient.Connections;
using CassandraClient.Core;
using CassandraClient.Helpers;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.ConnectionTests
{
    public class ClusterConnectionTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            commandExecuter = GetMock<ICommandExecuter>();
            clusterConnection = new ClusterConnection(commandExecuter, ConsistencyLevel.ALL,
                                                      ConsistencyLevel.EACH_QUORUM);
        }

        [Test]
        public void TestAddKeyspace()
        {
            var keyspace = new Keyspace
                {
                    ColumnFamilies = new Dictionary<string, ColumnFamily>
                        {
                            {"a", new ColumnFamily { Name = "t"}}
                        },
                    Name = "name",
                    ReplicaPlacementStrategy = "strategy",
                    ReplicationFactor = 4556
                };

            commandExecuter.Expect(connection => connection.Execute(ARG.EqualsTo(new AquilesCommandAdaptor(new AddKeyspaceCommand
                {
                    ConsistencyLevel = AquilesConsistencyLevel.EACH_QUORUM,
                    KeyspaceDefinition = keyspace.ToAquilesKeyspace()
                }))));
            /*commandExecuter.Expect(connection => connection.Execute(Arg<SchemaAgreementCommand>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((SchemaAgreementCommand)invocation.Arguments[0])
                );*/
            commandExecuter.Expect(connection => connection.Execute(Arg<AquilesCommandAdaptor>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((AquilesCommandAdaptor)invocation.Arguments[0])
                );
            clusterConnection.AddKeyspace(keyspace);
        }

        [Test]
        public void TestRemoveKeyspace()
        {
            commandExecuter.Expect(connection => connection.Execute(ARG.EqualsTo(new AquilesCommandAdaptor(new DropKeyspaceCommand
                {
                    ConsistencyLevel =
                        AquilesConsistencyLevel.
                        EACH_QUORUM,
                    Keyspace = "keyspace"
                }))));
            /*commandExecuter.Expect(connection => connection.Execute(Arg<SchemaAgreementCommand>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((SchemaAgreementCommand)invocation.Arguments[0])
                );*/
            commandExecuter.Expect(connection => connection.Execute(Arg<AquilesCommandAdaptor>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((AquilesCommandAdaptor)invocation.Arguments[0])
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
            commandExecuter
                .Expect(connection => connection.Execute(ARG.EqualsTo(new AquilesCommandAdaptor(command))))
                .WhenCalled(
                    invocation =>
                    SetKeyspaces((AquilesCommandAdaptor)invocation.Arguments[0], new List<AquilesKeyspace>()));

            CollectionAssert.IsEmpty(clusterConnection.RetrieveKeyspaces());
        }

        [Test]
        public void TestRetrieveKeyspacesWhenNullKeyspaces()
        {
            var command = new RetrieveKeyspacesCommand
                {
                    ConsistencyLevel = AquilesConsistencyLevel.ALL
                };
            commandExecuter
                .Expect(connection => connection.Execute(ARG.EqualsTo(new AquilesCommandAdaptor(command))))
                .WhenCalled(invocation => SetKeyspaces((AquilesCommandAdaptor)invocation.Arguments[0], null));

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
            commandExecuter
                .Expect(connection => connection.Execute(ARG.EqualsTo(new AquilesCommandAdaptor(command))))
                .WhenCalled(invocation => SetKeyspaces((AquilesCommandAdaptor)invocation.Arguments[0], keyspaces));

            var expectedResult = new List<Keyspace>(new[]
                {
                    new Keyspace
                        {
                            Name = "testName",
                            ReplicationFactor = 34232,
                            ColumnFamilies = new Dictionary<string, ColumnFamily>
                                {
                                    {"a", new ColumnFamily {Name = "b"}},
                                    {"d", new ColumnFamily {Name = "e"}}
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

        private static void SetKeyspaces(AquilesCommandAdaptor command, List<AquilesKeyspace> keyspaces)
        {
            Type retrieveKeyspaceCommandType = typeof(RetrieveKeyspacesCommand);
            PropertyInfo propertyInfo = retrieveKeyspaceCommandType.GetProperty("Keyspaces");
            propertyInfo.SetValue(command.command, keyspaces, null);
        }

        private static object SetOutput(AquilesCommandAdaptor command)
        {
            ;
            var setMethod = (typeof(SchemaAgreementCommand)).GetProperty("Output").GetSetMethod(true);
            return setMethod.Invoke(command.command, new[] { new Dictionary<string, List<string>> { { "zzz", null } } });
        }

        private ICommandExecuter commandExecuter;
        private ClusterConnection clusterConnection;
    }
}