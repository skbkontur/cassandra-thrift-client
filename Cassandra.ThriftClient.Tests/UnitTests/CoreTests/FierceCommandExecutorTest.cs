using Apache.Cassandra;

using Moq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace Cassandra.ThriftClient.Tests.UnitTests.CoreTests
{
    public class FierceCommandExecutorTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            fierceConnectionPool = GetMock<IPoolSet<IThriftConnection, string>>();
            cassandraClusterSettings = GetMock<ICassandraClusterSettings>();
            cassandraClusterSettings.Setup(x => x.EnableMetrics).Returns(false);
            executor = new FierceCommandExecutor(fierceConnectionPool.Object, cassandraClusterSettings.Object);
            var commandMock = GetMock<IFierceCommand>();
            command = commandMock.Object;
            commandMock.Setup(x => x.Name).Returns("commandName");
            commandMock.Setup(command1 => command1.CommandContext).Returns(new CommandContext {KeyspaceName = "keyspace"});
        }

        [Test]
        public void FiercedOkTest()
        {
            cassandraClusterSettings.Setup(settings => settings.Attempts).Returns(1);

            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;
            fierceConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection);
            thriftConnectionMock.Setup(connection => connection.ExecuteCommand(command));
            fierceConnectionPool.Setup(pool => pool.Release(thriftConnection));
            fierceConnectionPool.Setup(manager => manager.Good(thriftConnection));

            executor.Execute(command);
        }

        [Test]
        public void FiercedExceptionTest()
        {
            var thriftConnectionMock = GetMock<IThriftConnection>();
            var thriftConnection = thriftConnectionMock.Object;
            fierceConnectionPool.Setup(pool => pool.Acquire("keyspace")).Returns(thriftConnection);
            thriftConnectionMock.Setup(connection => connection.ExecuteCommand(command)).Throws(new TimedOutException());
            fierceConnectionPool.Setup(pool => pool.Remove(thriftConnection));
            fierceConnectionPool.Setup(pool => pool.Bad(thriftConnection));

            RunMethodWithException<CassandraClientTimedOutException>(() => executor.Execute(command), "Failed to execute cassandra command commandName in pool");
        }

        private Mock<ICassandraClusterSettings> cassandraClusterSettings;
        private IFierceCommand command;
        private Mock<IPoolSet<IThriftConnection, string>> fierceConnectionPool;
        private ICommandExecutor<IFierceCommand> executor;
    }
}