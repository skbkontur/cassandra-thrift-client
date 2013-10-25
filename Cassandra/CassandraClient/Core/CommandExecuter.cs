using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class CommandExecuter : ICommandExecuter
    {
        public CommandExecuter(IReplicaSetPool<IThriftConnection, ConnectionKey, IPEndPoint> clusterConnectionPool,
                               ICassandraClusterSettings settings)
        {
            this.clusterConnectionPool = clusterConnectionPool;
            this.settings = settings;
        }

        public Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge> GetKnowledges()
        {
            return new Dictionary<ConnectionPoolKey, KeyspaceConnectionPoolKnowledge>();
        }

        public void CheckConnections()
        {
        }

        public void Execute(Func<int, ICommand> createCommand)
        {
            var stopwatch = Stopwatch.StartNew();
            var command = createCommand(0);
            logger.DebugFormat("Start executing {0} command.", command.Name);
            try
            {
                for(var i = 0; i < settings.Attempts; ++i)
                {
                    IThriftConnection connectionInPool = null;
                    try
                    {
                        connectionInPool = clusterConnectionPool.Acquire(new ConnectionKey(command.CommandContext.KeyspaceName, command.IsFierce));
                        try
                        {
                            connectionInPool.ExecuteCommand(command);
                            clusterConnectionPool.Good((connectionInPool as ThriftConnectionInPoolWrapper).ReplicaKey);
                            return;
                        }
                        finally
                        {
                            clusterConnectionPool.Release(connectionInPool);
                        }
                    }
                    catch(Exception e)
                    {
                        var message = string.Format("An error occurred while executing cassandra command '{0}'", command.Name);

                        var exception = CassandraExceptionTransformer.Transform(e, message);

                        if(connectionInPool != null)
                            logger.WarnFormat(string.Format("Attempt {0} on {1} failed.", i, (connectionInPool as ThriftConnectionInPoolWrapper).ReplicaKey), exception);
                        else
                            logger.WarnFormat(string.Format("Attempt {0} to all nodes failed.", i), exception);

                        if(connectionInPool != null)
                            clusterConnectionPool.Bad((connectionInPool as ThriftConnectionInPoolWrapper).ReplicaKey);

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
            clusterConnectionPool.Dispose();
        }

        private readonly ConcurrentDictionary<string, TimeStatistics> timeStatisticsDictionary = new ConcurrentDictionary<string, TimeStatistics>();
        private readonly IReplicaSetPool<IThriftConnection, ConnectionKey, IPEndPoint> clusterConnectionPool;
        private readonly ICassandraClusterSettings settings;
        private readonly ILog logger = LogManager.GetLogger(typeof(CommandExecuter));
    }
}