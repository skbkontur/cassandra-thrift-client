using System;
using System.IO;

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
    public class CommandExecutorTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            dataConnectionPool = GetMock<IPoolSet<IThriftConnection, string>>();
            cassandraClusterSettings = GetMock<ICassandraClusterSettings>();
            cassandraClusterSettings.Expect(x => x.EnableMetrics).Return(false).Repeat.Any();
            executor = new SimpleCommandExecutor(dataConnectionPool, cassandraClusterSettings);
            command = GetMock<ISimpleCommand>();
            command.Expect(x => x.Name).Return("commandName").Repeat.Any();
            command.Expect(command1 => command1.CommandContext).Return(new CommandContext {KeyspaceName = "keyspace"}).Repeat.Any();
        }

        [Test]
        public void ZeroAttemptsTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(0);
            executor.Execute(command);
        }

        [Test]
        public void ExecuteOkTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2);

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(thriftConnection));

            executor.Execute(command);
        }

        [Test]
        public void RetryableExceptionTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new TimedOutException());
            dataConnectionPool.Expect(pool => pool.Remove(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            var goodThriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(goodThriftConnection);
            goodThriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(goodThriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(goodThriftConnection));

            executor.Execute(command);
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
                )] Type commandExecutionException)
        {
            InternalTestReleaseConnectionOnException((Exception)Activator.CreateInstance(commandExecutionException), true, true);
        }

        [Test, Sequential]
        public void TestHandleExceptionWithCorrectConnectionAndGoodReplica(
            [Values(
                typeof(NotFoundException),
                typeof(UnavailableException)
                )] Type commandExecutionException)
        {
            InternalTestReleaseConnectionOnException((Exception)Activator.CreateInstance(commandExecutionException), false, false);
        }

        [Test, Sequential]
        public void TestHandleExceptionWithNoTransformationToAttemptsException(
            [Values(
                typeof(InvalidRequestException),
                typeof(AuthenticationException),
                typeof(AuthorizationException),
                typeof(SchemaDisagreementException)
                )] Type commandExecutionException,
            [Values(
                typeof(CassandraClientInvalidRequestException),
                typeof(CassandraClientAuthenticationException),
                typeof(CassandraClientAuthorizationException),
                typeof(CassandraClientSchemaDisagreementException)
                )] Type excpectedExceptionType
            )
        {
            InternalTestExceptionTransformation((Exception)Activator.CreateInstance(commandExecutionException), excpectedExceptionType);
        }

        [Test, Sequential]
        public void TestHandleExceptionWithTransformationToAttemptsException(
            [Values(
                typeof(NotFoundException),
                typeof(UnavailableException),
                typeof(TimedOutException),
                typeof(TProtocolException),
                typeof(TApplicationException),
                typeof(TTransportException),
                typeof(IOException),
                typeof(Exception)
                )] Type commandExecutionException
            )
        {
            InternalTestExceptionTransformation((Exception)Activator.CreateInstance(commandExecutionException), typeof(CassandraAttemptsException));
        }

        [Test]
        public void AttemptsExceptionTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new TimedOutException());
            dataConnectionPool.Expect(pool => pool.Remove(thriftConnection));
            dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            RunMethodWithException<CassandraAttemptsException>(() => executor.Execute(command), "Operation failed for 1 attempts");
        }

        private void InternalTestReleaseConnectionOnException(Exception commandExecutionException, bool reduceReplicaLive, bool removeConnection)
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(commandExecutionException);

            if(removeConnection)
                dataConnectionPool.Expect(pool => pool.Remove(thriftConnection));
            else
                dataConnectionPool.Expect(pool => pool.Release(thriftConnection));

            if(reduceReplicaLive)
                dataConnectionPool.Expect(pool => pool.Bad(thriftConnection));
            else
                dataConnectionPool.Expect(pool => pool.Good(thriftConnection));

            var goodThriftConnection = GetMock<IThriftConnection>();
            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(goodThriftConnection);
            goodThriftConnection.Expect(connection => connection.ExecuteCommand(command));
            dataConnectionPool.Expect(pool => pool.Release(goodThriftConnection));
            dataConnectionPool.Expect(pool => pool.Good(goodThriftConnection));

            executor.Execute(command);
        }

        private void InternalTestExceptionTransformation(Exception commandExecutionException, Type expectedException)
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(2).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();

            dataConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection).Repeat.Any();
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(commandExecutionException).Repeat.Any();

            dataConnectionPool.Expect(pool => pool.Remove(thriftConnection)).Repeat.Any();
            dataConnectionPool.Expect(pool => pool.Release(thriftConnection)).Repeat.Any();

            dataConnectionPool.Expect(pool => pool.Bad(thriftConnection)).Repeat.Any();
            dataConnectionPool.Expect(pool => pool.Good(thriftConnection)).Repeat.Any();

            Assert.That(() => executor.Execute(command), Throws.Exception.InstanceOf(expectedException));
        }

        private ICassandraClusterSettings cassandraClusterSettings;
        private ISimpleCommand command;
        private IPoolSet<IThriftConnection, string> dataConnectionPool;
        private ICommandExecutor<ISimpleCommand> executor;
    }
}