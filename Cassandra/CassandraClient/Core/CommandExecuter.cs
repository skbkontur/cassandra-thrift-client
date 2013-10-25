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
        public CommandExecuter(ReplicaSetPool<ThriftConnectionWrapper, ConnectionKey, IPEndPointWrapper> clusterConnectionPool,
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
                    ThriftConnectionWrapper connection = null;
                    try
                    {
                        connection = clusterConnectionPool.Acquire(new ConnectionKey(command.CommandContext.KeyspaceName, command.IsFierce));
                        try
                        {
                            connection.ExecuteCommand(command);
                            clusterConnectionPool.Good(connection.ReplicaKey);
                            return;
                        }
                        finally 
                        {
                            clusterConnectionPool.Release(connection);
                        }
                    }
                    catch(Exception e)
                    {
                        var message = string.Format("An error occurred while executing cassandra command '{0}'", command.Name);

                        var exception = CassandraExceptionTransformer.Transform(e, message);

                        if(connection != null)
                            logger.WarnFormat(string.Format("Attempt {0} on {1} failed.", i, connection.PoolKey), exception);
                        else
                            logger.WarnFormat(string.Format("Attempt {0} to all nodes failed.", i), exception);

                        if(connection != null)
                            clusterConnectionPool.Bad(connection.ReplicaKey);

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
        private readonly ReplicaSetPool<ThriftConnectionWrapper, ConnectionKey, IPEndPointWrapper> clusterConnectionPool;
        private readonly ICassandraClusterSettings settings;
        private readonly ILog logger = LogManager.GetLogger(typeof(CommandExecuter));
    }

    public class IPEndPointWrapper : IEquatable<IPEndPointWrapper>
    {
        public IPEndPointWrapper(IPEndPoint value)
        {
            Value = value;
        }

        public bool Equals(IPEndPointWrapper other)
        {
            if(ReferenceEquals(null, other)) return false;
            if(ReferenceEquals(this, other)) return true;
            return Equals(Value, other.Value);
        }

        public override bool Equals(object obj)
        {
            if(ReferenceEquals(null, obj)) return false;
            if(ReferenceEquals(this, obj)) return true;
            if(obj.GetType() != this.GetType()) return false;
            return Equals((IPEndPointWrapper)obj);
        }

        public override int GetHashCode()
        {
            return (Value != null ? Value.GetHashCode() : 0);
        }

        public IPEndPoint Value { get; set; }
    }

    public class ThriftConnectionWrapper : IDisposable, IPoolKeyContainer<ConnectionKey, IPEndPointWrapper>, ILiveness
    {
        public ThriftConnectionWrapper(ConnectionKey poolKey, IPEndPointWrapper ipEndPointWrapper, int timeout, IPEndPoint ipEndPoint, string keyspaceName)
        {
            thriftConnection = new ThriftConnection(timeout, ipEndPoint, keyspaceName);
            PoolKey = poolKey;
            ReplicaKey = ipEndPointWrapper;
        }

        public void ExecuteCommand(ICommand command)
        {
            thriftConnection.ExecuteCommand(command);
        }

        public void Dispose()
        {
            thriftConnection.Dispose();
        }

        public bool IsAlive { get { return thriftConnection.IsAlive; } }
        private readonly ThriftConnection thriftConnection;
        public ConnectionKey PoolKey { get; private set; }
        public IPEndPointWrapper ReplicaKey { get; private set; }
    }
}