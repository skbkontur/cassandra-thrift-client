using System;
using System.IO;
using System.Net;

using Apache.Cassandra;

using NUnit.Framework;

using Rhino.Mocks;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Cassandra.Tests.CoreTests
{
    public class CommandExecuterTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            dataConnectionPool = GetMock<IPoolSet<IThriftConnection, string>>();
            fierceConnectionPool = GetMock<IPoolSet<IThriftConnection, string>>();
            cassandraClusterSettings = GetMock<ICassandraClusterSettings>();
            executer = new CommandExecuter(dataConnectionPool, fierceConnectionPool, cassandraClusterSettings);
            command = GetMock<ICommand>();
            command.Expect(x => x.Name).Return("commandName").Repeat.Any();
            command.Expect(command1 => command1.CommandContext).Return(new CommandContext { KeyspaceName = "keyspace" }).Repeat.Any();
        }

        [Test]
        public void ZeroAttemptsTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(0);
            executer.Execute(command);
        }

        [Test]
        public void ExecuteOkTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(thriftConnection));

            executer.Execute(command);
        }

        [Test]
        public void RetryableExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new InvalidRequestException("xxx"));
            dataConnectionPool.Expect(pool => pool.Release(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            var goodThriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(goodThriftConnection);
            goodThriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(goodThriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(goodThriftConnection));

            executer.Execute(command);
        }

        
        [Test, Sequential]
        public void TestHandleExceptionWithCorruptConnectionAndBadReplica(
            [Values(
                typeof(TimedOutException),
                typeof(TProtocolException),
                typeof(TApplicationException),
                typeof(TTransportException),
                typeof(IOException),
                typeof(Exception)
            )]
            Type commandExecutionException)
        {
            InternalTestReleaseConnectionOnException((Exception)Activator.CreateInstance(commandExecutionException), true, true);
        }

        [Test, Sequential]
        public void TestHandleExceptionWithCorrectConnectionAndGoodReplica(
            [Values(
                typeof(InvalidRequestException),
                typeof(NotFoundException),
                typeof(UnavailableException),
                typeof(AuthenticationException),
                typeof(AuthorizationException),
                typeof(SchemaDisagreementException)
                )]            
            Type commandExecutionException)
        {
            InternalTestReleaseConnectionOnException((Exception)Activator.CreateInstance(commandExecutionException), false, false);
        }

        private void InternalTestReleaseConnectionOnException(Exception commandExecutionException, bool reduceReplicaLive, bool removeConnection)
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(commandExecutionException);
            
            if (removeConnection)
                dataConnectionPool.Expect(pool => pool.Remove(thriftConnection));
            else
                dataConnectionPool.Expect(pool => pool.Release(thriftConnection));

            if (reduceReplicaLive)
                dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));
            else
                dataConnectionPool.Expect(pool => pool.Good(thriftConnection));

            var goodThriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(goodThriftConnection);
            goodThriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(goodThriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(goodThriftConnection));

            executer.Execute(command);
        }

        [Test]
        public void TestRemoveConnectionIfPoolThrowExceptionInGoodMethod()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            dataConnectionPool.Expect(pool => pool.Good(thriftConnection)).Throw(new Exception("xxx"));
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Remove(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            var goodThriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(goodThriftConnection);
            goodThriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(goodThriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(goodThriftConnection));

            executer.Execute(command);
        }

        [Test]
        public void AttemptsExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(false).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new IOException("xxx"));
            dataConnectionPool.Expect(pool => pool.Release(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            RunMethodWithException<CassandraAttemptsException>(() => executer.Execute(command), "Operation failed for 1 attempts");
        }

        [Test]
        public void FiercedTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(true).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            fierceConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            fierceConnectionPool.Expect(pool => pool.Release(thriftConnection));
            fierceConnectionPool.Expect(manager => manager.Good(thriftConnection));

            executer.Execute(command);
        }

        [Test]
        public void FiercedRetryableExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(true).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            fierceConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new InvalidRequestException("xxx"));
            fierceConnectionPool.Expect(pool => pool.Release(thriftConnection));
            fierceConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            var goodThriftConnection = GetMock<IThriftConnection>();
            fierceConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(goodThriftConnection);
            goodThriftConnection.Expect(connection => connection.ExecuteCommand(command));
            fierceConnectionPool.Expect(pool => pool.Release(goodThriftConnection));
            fierceConnectionPool.Expect(pool => pool.Good(goodThriftConnection));

            executer.Execute(command);
        }

        [Test]
        public void FiercedAttemptsExceptionTest()
        {
            command.Expect(command1 => command1.IsFierce).Return(true).Repeat.Any();
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            fierceConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new InvalidRequestException("xxx"));
            fierceConnectionPool.Expect(pool => pool.Release(thriftConnection));
            fierceConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            RunMethodWithException<CassandraAttemptsException>(() => executer.Execute(command), "Operation failed for 1 attempts");
        }

        private CommandExecuter executer;
        private ICassandraClusterSettings cassandraClusterSettings;
        private ICommand command;
        private IPoolSet<IThriftConnection, string> dataConnectionPool;
        private IPoolSet<IThriftConnection, string> fierceConnectionPool;
    }
}