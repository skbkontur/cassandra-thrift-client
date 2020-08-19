using System;
using System.IO;

using Apache.Cassandra;

using Moq;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Core;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool;
using SkbKontur.Cassandra.ThriftClient.Exceptions;

using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Tests.UnitTests.CoreTests
{
    public class CommandExecutorTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            dataConnectionPool = GetMock<IPoolSet<IThriftConnection, string>>();
            var commandMock = GetMock<ISimpleCommand>();
            command = commandMock.Object;
            commandMock.SetupGet(x => x.Name).Returns("commandName");
            commandMock.SetupGet(command1 => command1.CommandContext).Returns(new CommandContext {KeyspaceName = "keyspace"});
        }

        private void SetUpExecutor(int attempts)
        {
            cassandraClusterSettings = GetMock<ICassandraClusterSettings>();
            cassandraClusterSettings.SetupGet(x => x.EnableMetrics).Returns(false);
            cassandraClusterSettings.SetupGet(settings => settings.Attempts).Returns(attempts);
            executor = new SimpleCommandExecutor(dataConnectionPool.Object, cassandraClusterSettings.Object, new SilentLog());
        }

        [Test]
        public void ExecuteOkTest()
        {
            SetUpExecutor(attempts : 1);

            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;
            dataConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection).Verifiable();
            thriftConnectionMock.Setup(connection => connection.ExecuteCommand(command)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Release(thriftConnection)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Good(thriftConnection)).Verifiable();

            executor.Execute(command);
        }

        [Test]
        public void RetryableExceptionTest()
        {
            SetUpExecutor(attempts : 2);

            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;
            thriftConnectionMock.SetupSequence(connection => connection.ExecuteCommand(command))
                                .Throws(new TimedOutException())
                                .Pass();

            dataConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection).Verifiable();
            dataConnectionPool.Setup(pool => pool.Remove(thriftConnection)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Bad(thriftConnection)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Release(thriftConnection)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Good(thriftConnection)).Verifiable();

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
            SetUpExecutor(attempts : 1);

            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;
            dataConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection).Verifiable();
            thriftConnectionMock.Setup(connection => connection.ExecuteCommand(command)).Throws(new TimedOutException()).Verifiable();
            dataConnectionPool.Setup(pool => pool.Remove(thriftConnection)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Bad(thriftConnection)).Verifiable();

            RunMethodWithException<CassandraAttemptsException>(() => executor.Execute(command), "Operation failed for 1 attempts");
        }

        private void InternalTestReleaseConnectionOnException(Exception commandExecutionException, bool reduceReplicaLive, bool removeConnection)
        {
            SetUpExecutor(attempts : 2);

            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;
            dataConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection).Verifiable();
            thriftConnectionMock.SetupSequence(connection => connection.ExecuteCommand(command))
                                .Throws(commandExecutionException)
                                .Pass();

            if (removeConnection)
                dataConnectionPool.Setup(pool => pool.Remove(thriftConnection)).Verifiable();
            else
                dataConnectionPool.Setup(pool => pool.Release(thriftConnection)).Verifiable();

            if (reduceReplicaLive)
                dataConnectionPool.Setup(pool => pool.Bad(thriftConnection)).Verifiable();
            else
                dataConnectionPool.Setup(pool => pool.Good(thriftConnection)).Verifiable();

            dataConnectionPool.Setup(pool => pool.Release(thriftConnection)).Verifiable();
            dataConnectionPool.Setup(pool => pool.Good(thriftConnection)).Verifiable();

            executor.Execute(command);
        }

        private void InternalTestExceptionTransformation(Exception commandExecutionException, Type expectedException)
        {
            SetUpExecutor(attempts : 2);

            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;

            dataConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection);
            thriftConnectionMock.Setup(connection => connection.ExecuteCommand(command)).Throws(commandExecutionException);

            dataConnectionPool.Setup(pool => pool.Remove(thriftConnection));
            dataConnectionPool.Setup(pool => pool.Release(thriftConnection));

            dataConnectionPool.Setup(pool => pool.Bad(thriftConnection));
            dataConnectionPool.Setup(pool => pool.Good(thriftConnection));

            Assert.That(() => executor.Execute(command), Throws.Exception.InstanceOf(expectedException));
        }

        private Mock<ICassandraClusterSettings> cassandraClusterSettings;
        private ISimpleCommand command;
        private Mock<IPoolSet<IThriftConnection, string>> dataConnectionPool;
        private ICommandExecutor<ISimpleCommand> executor;
    }
}