using System;
using System.IO;
using System.Net;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Core;
using CassandraClient.Core.Pools;
using CassandraClient.Exceptions;

using NUnit.Framework;

using Rhino.Mocks;

namespace Cassandra.Tests.CoreTests
{
    public class CommandExecuterTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            clusterConnectionPool = GetMock<IClusterConnectionPool>();
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
            endpointManager.Expect(manager => manager.Register(ipEndPoint1));
            endpointManager.Expect(manager => manager.Register(ipEndPoint2));
            endpointManager.Expect(manager => manager.Register(ipEndPoint3));
            executer = new CommandExecuter(clusterConnectionPool, endpointManager, cassandraClusterSettings);
        }

        [Test]
        public void ValidateFailTest()
        {
            var command = GetMock<ICommand>();
            command.Expect(command1 => command1.Validate()).Return(ValidationResult.Error("message"));
            RunMethodWithException<CassandraClientInvalidRequestException>(() => executer.Execute(command), "message");
        }

        [Test]
        public void ZeroAttemptsTest()
        {
            var command = GetMock<ICommand>();
            command.Expect(command1 => command1.Validate()).Return(ValidationResult.Ok());
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(0);
            executer.Execute(command);
        }

        [Test]
        public void ExecuteOkTest()
        {
            var command = GetMock<ICommand>();
            command.Expect(command1 => command1.Validate()).Return(ValidationResult.Ok());
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            endpointManager.Expect(manager => manager.GetEndPoint()).Return(ipEndPoint1);
            command.Expect(command1 => command1.Keyspace).Return("keyspace");
            var thriftConnection = GetMock<IThriftConnection>();
            clusterConnectionPool.Expect(pool => pool.BorrowConnection(ipEndPoint1, "keyspace")).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Good(ipEndPoint1));
            executer.Execute(command);
        }

        [Test]
        public void UnretryableExceptionTest()
        {
            var command = GetMock<ICommand>();
            command.Expect(command1 => command1.Validate()).Return(ValidationResult.Ok());
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            endpointManager.Expect(manager => manager.GetEndPoint()).Return(ipEndPoint1);
            command.Expect(command1 => command1.Keyspace).Return("keyspace");
            var thriftConnection = GetMock<IThriftConnection>();
            clusterConnectionPool.Expect(pool => pool.BorrowConnection(ipEndPoint1, "keyspace")).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new Exception("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            RunMethodWithException<CassandraUnknownException>(() => executer.Execute(command), string.Format("An error occured while executing cassandra command '{0}'", command.GetType()));
        }

        [Test]
        public void RetryableExceptionTest()
        {
            var command = GetMock<ICommand>();
            command.Expect(command1 => command1.Validate()).Return(ValidationResult.Ok());
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            endpointManager.Expect(manager => manager.GetEndPoint()).Return(ipEndPoint1);
            command.Expect(command1 => command1.Keyspace).Return("keyspace");
            var thriftConnection = GetMock<IThriftConnection>();
            clusterConnectionPool.Expect(pool => pool.BorrowConnection(ipEndPoint1, "keyspace")).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            endpointManager.Expect(manager => manager.GetEndPoint()).Return(ipEndPoint2);
            command.Expect(command1 => command1.Keyspace).Return("keyspace");
            clusterConnectionPool.Expect(pool => pool.BorrowConnection(ipEndPoint2, "keyspace")).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Good(ipEndPoint2));
            executer.Execute(command);
        }

        [Test]
        public void AttemptsExceptionTest()
        {
            var command = GetMock<ICommand>();
            command.Expect(command1 => command1.Validate()).Return(ValidationResult.Ok());
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            endpointManager.Expect(manager => manager.GetEndPoint()).Return(ipEndPoint1);
            command.Expect(command1 => command1.Keyspace).Return("keyspace");
            var thriftConnection = GetMock<IThriftConnection>();
            clusterConnectionPool.Expect(pool => pool.BorrowConnection(ipEndPoint1, "keyspace")).
                Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            thriftConnection.Expect(connection => connection.Dispose());
            endpointManager.Expect(manager => manager.Bad(ipEndPoint1));
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1);
            RunMethodWithException<CassandraAttemptsException>(() => executer.Execute(command), "Operation failed for 1 attempts");
        }

        private CommandExecuter executer;
        private ICassandraClusterSettings cassandraClusterSettings;
        private IEndpointManager endpointManager;
        private IClusterConnectionPool clusterConnectionPool;
        private IPEndPoint ipEndPoint3;
        private IPEndPoint ipEndPoint1;
        private IPEndPoint ipEndPoint2;
    }
}