using Apache.Cassandra;

using NUnit.Framework;

using Rhino.Mocks;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace Cassandra.Tests.CoreTests
{
    public class FierceCommandExecutorTest : TestBase
    {
        public override void SetUp()
        {
            base.SetUp();
            fierceConnectionPool = GetMock<IPoolSet<IThriftConnection, string>>();
            cassandraClusterSettings = GetMock<ICassandraClusterSettings>();
            cassandraClusterSettings.Expect(x => x.EnableMetrics).Return(false).Repeat.Any();
            executor = new FierceCommandExecutor(fierceConnectionPool, cassandraClusterSettings);
            command = GetMock<IFierceCommand>();
            command.Expect(x => x.Name).Return("commandName").Repeat.Any();
            command.Expect(command1 => command1.CommandContext).Return(new CommandContext {KeyspaceName = "keyspace"}).Repeat.Any();
        }

        [Test]
        public void FiercedOkTest()
        {
            cassandraClusterSettings.Expect(settings => settings.Attempts).Return(1).Repeat.Any();

            var thriftConnection = GetMock<IThriftConnection>();
            fierceConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command));
            fierceConnectionPool.Expect(pool => pool.Release(thriftConnection));
            fierceConnectionPool.Expect(manager => manager.Good(thriftConnection));

            executor.Execute(command);
        }

        [Test]
        public void FiercedExceptionTest()
        {
            var thriftConnection = GetMock<IThriftConnection>();
            fierceConnectionPool.Expect(pool => pool.Acquire("keyspace")).Return(thriftConnection);
            thriftConnection.Expect(connection => connection.ExecuteCommand(command)).Throw(new TimedOutException());
            fierceConnectionPool.Expect(pool => pool.Remove(thriftConnection));
            fierceConnectionPool.Expect(pool => pool.Bad(thriftConnection));

            RunMethodWithException<CassandraClientTimedOutException>(() => executor.Execute(command), "An error occurred while executing cassandra command 'commandName'");
        }

        private ICassandraClusterSettings cassandraClusterSettings;
        private IFierceCommand command;
        private IPoolSet<IThriftConnection, string> fierceConnectionPool;
        private ICommandExecutor<IFierceCommand> executor;
    }
}