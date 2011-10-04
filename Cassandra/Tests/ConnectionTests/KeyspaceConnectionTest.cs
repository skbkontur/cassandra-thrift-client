using System;
using System.Collections.Generic;
using System.Reflection;

using CassandraClient.AquilesTrash.Command;
using CassandraClient.AquilesTrash.Command.System;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.Abstractions;
using CassandraClient.Connections;
using CassandraClient.Core;

using NUnit.Framework;
using NUnit.Framework.SyntaxHelpers;

using Rhino.Mocks;

namespace Cassandra.Tests.ConnectionTests
{
    public class KeyspaceConnectionTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            commandExecuter = GetMock<ICommandExecuter>();
            keyspaceConnection = new KeyspaceConnection(commandExecuter, ConsistencyLevel.ALL,
                                                        ConsistencyLevel.EACH_QUORUM, keyspaceName);
        }

        [Test]
        public void TestAddColumnFamily()
        {
            const string keyspace = "keyspace";
            const string columnFamilyName = "familyName";
            commandExecuter.Expect(c => c.Execute(Arg<AquilesCommandAdaptor>.Is.TypeOf)).WhenCalled(
                i =>
                    {
                        var command = (AquilesCommandAdaptor)i.Arguments[0];
                        Assert.That(((AddColumnFamilyCommand)command.command).ColumnFamilyDefinition.Name, Is.EqualTo(columnFamilyName));
                        Assert.That(((AddColumnFamilyCommand)command.command).ColumnFamilyDefinition.Keyspace, Is.EqualTo(keyspace));
                        Assert.That(((AddColumnFamilyCommand)command.command).ConsistencyLevel, Is.EqualTo(AquilesConsistencyLevel.EACH_QUORUM));
                    });
            commandExecuter.Expect(connection => connection.Execute(Arg<AquilesCommandAdaptor>.Is.TypeOf)).WhenCalled(
                invocation => SetOutput((AquilesCommandAdaptor)invocation.Arguments[0])
                );
            keyspaceConnection.AddColumnFamily(columnFamilyName);
        }

        [Test]
        public void TestDescribeKeyspace()
        {
            commandExecuter.Expect(c => c.Execute(Arg<AquilesCommandAdaptor>.Is.TypeOf)).WhenCalled(
                invocation => Qxx((AquilesCommandAdaptor)invocation.Arguments[0]));

            Keyspace keyspace = keyspaceConnection.DescribeKeyspace();
            Assert.That(keyspace.ColumnFamilies.Count, Is.EqualTo(2));
            Assert.That(keyspace.ColumnFamilies["CFName1"].Name, Is.EqualTo("CFName1"));
            Assert.That(keyspace.ColumnFamilies["CFName2"].Name, Is.EqualTo("CFName2"));

            Assert.That(keyspace.Name, Is.EqualTo(keyspaceName));
            Assert.That(keyspace.ReplicaPlacementStrategy, Is.EqualTo(replicationPlacementStrategy));
            Assert.That(keyspace.ReplicationFactor, Is.EqualTo(replicationFactor));
        }

        private static void Qxx(AquilesCommandAdaptor command)
        {
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
            propertyInfo.SetValue(command.command, aquilesKeyspace, null);
        }

        private static object SetOutput(AquilesCommandAdaptor command)
        {
            MethodInfo setMethod = (typeof(SchemaAgreementCommand)).GetProperty("Output").GetSetMethod(true);
            return setMethod.Invoke(command.command, new[] {new Dictionary<string, List<string>> {{"zzz", null}}});
        }

        private ICommandExecuter commandExecuter;
        private KeyspaceConnection keyspaceConnection;
        private const string keyspaceName = "keyspace";
        const int replicationFactor = 5;
        const string replicationPlacementStrategy = "ReplicationPlacementStrategy";
    }
}