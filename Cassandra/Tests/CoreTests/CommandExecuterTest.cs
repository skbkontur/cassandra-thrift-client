using System.IO;
using System.Net;

using NUnit.Framework;

using Rhino.Mocks;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace Cassandra.Tests.CoreTests
{
    public class CommandExecuterTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();

            clusterConnectionPool = GetMock<ReplicaSetPool<ThriftConnectionWrapper, ConnectionKey, IPEndPointWrapper>>();
            endpointManager = GetMock<IEndpointManager>();
            cassandraClusterSettings = GetMock<ICassandraClusterSettings>();
            ipEndPoint1 = new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 1}), 1221);
            ipEndPoint2 = new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 1}), 1222);
            ipEndPoint3 = new IPEndPoint(new IPAddress(new byte[] {1, 1, 1, 1}), 1223);
            cassandraClusterSettings.Expect(settings => settings.Endpoints).Return(new[]
                {
                    ipEndPoint1,
                    ipEndPoint2,
                    ipEndPoint3
                });
            cassandraClusterSettings.Expect(settings => settings.EndpointForFierceCommands).Return(ipEndPoint1);
            clusterConnectionPool.Expect(manager => manager.RegisterKey(new IPEndPointWrapper(ipEndPoint1))).Repeat.Times(2);
            clusterConnectionPool.Expect(manager => manager.RegisterKey(new IPEndPointWrapper(ipEndPoint2)));
            clusterConnectionPool.Expect(manager => manager.RegisterKey(new IPEndPointWrapper(ipEndPoint3)));
            executer = new CommandExecuter(clusterConnectionPool, cassandraClusterSettings);

            command = GetMock<ICommand>();
            command.Expect(x => x.Name).Return("commandName").Repeat.Any();
            command.Expect(command1 => command1.CommandContext).Return(new CommandContext { KeyspaceName = "keyspace" }).Repeat.Any();
        }

        [Test]
        public void ZeroAttemptsTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(0);
            executer.Execute(command);
        }

        [Test]
        public void ExecuteOkTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Times(2);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            endpointManager.Expect(manager => manager.GetEndPoints()).Return(new[] {ipEndPoint1});
            var thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Good(ipEndPoint1));
            executer.Execute(command);
        }

        [Test]
        public void RetryableExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Times(3);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            endpointManager.Expect(manager => manager.GetEndPoints()).Return(new[] {ipEndPoint1, ipEndPoint2});
            var thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Good(ipEndPoint2));
            executer.Execute(command);
        }

        [Test]
        public void AttemptsExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Times(2);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            endpointManager.Expect(manager => manager.GetEndPoints()).Return(new[] {ipEndPoint1});
            var thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            RunMethodWithException<CassandraAttemptsException>(() => executer.Execute(command), "Operation failed for 1 attempts");
        }

        [Test]
        public void FiercedTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(true).Repeat.Times(2);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            cassandraClusterSettings.Expect(settings => settings.EndpointForFierceCommands).Return(ipEndPoint1);
            var thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Good(ipEndPoint1));
            executer.Execute(command);
        }

        [Test]
        public void FiercedRetryableExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(true).Repeat.Times(4);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            cassandraClusterSettings.Expect(settings => settings.EndpointForFierceCommands).Return(ipEndPoint1);
            var thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            cassandraClusterSettings.Expect(settings => settings.EndpointForFierceCommands).Return(ipEndPoint2);
            thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Good(ipEndPoint2));
            executer.Execute(command);
        }

        [Test]
        public void FiercedAttemptsExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(true).Repeat.Times(2);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            cassandraClusterSettings.Expect(settings => settings.EndpointForFierceCommands).Return(ipEndPoint1);
            var thriftConnection = GetMock<ThriftConnectionWrapper>();
            clusterConnectionPool.Expect(pool => pool.Acquire(GetConnectionPoolKey("keyspace", false))).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            RunMethodWithException<CassandraAttemptsException>(() => executer.Execute(command), "Operation failed for 1 attempts");
        }

        private static ConnectionKey GetConnectionPoolKey(string keyspace, bool isFierce)
        {
            return new ConnectionKey
                (
                    keyspace,
                    isFierce
                );
        }

        private CommandExecuter executer;
        private ICassandraClusterSettings cassandraClusterSettings;
        private IEndpointManager endpointManager;
        private ReplicaSetPool<ThriftConnectionWrapper, ConnectionKey, IPEndPointWrapper> clusterConnectionPool;
        private IPEndPoint ipEndPoint3;
        private IPEndPoint ipEndPoint1;
        private IPEndPoint ipEndPoint2;
        private ICommand command;
    }
}