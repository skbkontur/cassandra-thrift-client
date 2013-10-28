using System;
using System.Collections.Concurrent;
using System.Diagnostics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    using IThriftConnectionReplicaSetPool = IPoolSet<IThriftConnection, string>;

    internal class CommandExecuter : ICommandExecuter
    {
        public CommandExecuter(
            IThriftConnectionReplicaSetPool dataCommandsConnectionPool,
            IThriftConnectionReplicaSetPool fierceCommandsConnectionPool,
            ICassandraClusterSettings settings)
        {
            this.dataCommandsConnectionPool = dataCommandsConnectionPool;
            this.fierceCommandsConnectionPool = fierceCommandsConnectionPool;
            this.settings = settings;
        }

        public void CheckConnections()
        {
        }

        public void Execute(Func<int, ICommand> createCommand)
        {
            var stopwatch = Stopwatch.StartNew();
            var command = createCommand(0);
            var pool = command.IsFierce ? fierceCommandsConnectionPool : dataCommandsConnectionPool;
            logger.DebugFormat("Start executing {0} command.", command.Name);
            try
            {
                for(var i = 0; i < settings.Attempts; ++i)
                {
                    IThriftConnection connectionInPool = null;
                    try
                    {
                        connectionInPool = pool.Acquire(command.CommandContext.KeyspaceName);
                        try
                        {
                            connectionInPool.ExecuteCommand(command);
                            pool.Good(connectionInPool);
                            return;
                        }
                        finally
                        {
                            pool.Release(connectionInPool);
                        }
                    }
                    catch(Exception e)
                    {
                        var message = string.Format("An error occurred while executing cassandra command '{0}'", command.Name);

                        var exception = CassandraExceptionTransformer.Transform(e, message);

                        if(connectionInPool != null)
                            logger.Warn(string.Format("Attempt {0} on {1} failed.", i, connectionInPool), exception);
                        else
                            logger.Warn(string.Format("Attempt {0} to all nodes failed.", i), exception);

                        if(connectionInPool != null)
                            pool.Bad(connectionInPool);

                        command = createCommand(i + 1);
                        if(i + 1 == settings.Attempts)
                            throw new CassandraAttemptsException(settings.Attempts, exception);
                    }
                }
            }
            finally
            {
                var timeStatisticsTitle = string.Format("Cassandra.{0}{1}", command.Name, command.CommandContext);
                var timeStatistics = timeStatisticsDictionary.GetOrAdd(timeStatisticsTitle, x => new TimeStatistics(timeStatisticsTitle));
                timeStatistics.AddTime(stopwatch.ElapsedMilliseconds);
            }
        }

        public void Execute(ICommand command)
        {
            Execute(attempt => command);
        }

        public void Dispose()
        {
            dataCommandsConnectionPool.Dispose();
            fierceCommandsConnectionPool.Dispose();
        }

        private readonly ConcurrentDictionary<string, TimeStatistics> timeStatisticsDictionary = new ConcurrentDictionary<string, TimeStatistics>();
        private readonly IThriftConnectionReplicaSetPool dataCommandsConnectionPool;
        private readonly IThriftConnectionReplicaSetPool fierceCommandsConnectionPool;
        private readonly ICassandraClusterSettings settings;
        private readonly ILog logger = LogManager.GetLogger(typeof(CommandExecuter));
    }
}