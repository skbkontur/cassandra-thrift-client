using System;

using JetBrains.Annotations;

using log4net;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Metrics;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    using IThriftConnectionReplicaSetPool = IPoolSet<IThriftConnection, string>;

    internal abstract class CommandExecutorBase<TCommand> : ICommandExecutor<TCommand>
        where TCommand : ICommand
    {
        protected CommandExecutorBase([NotNull] IThriftConnectionReplicaSetPool connectionPool,
                                      [NotNull] ICassandraClusterSettings settings)
        {
            this.connectionPool = connectionPool;
            this.settings = settings;
            logger = LogManager.GetLogger(GetType());
        }

        public abstract void Execute([NotNull] TCommand command);
        public abstract void Execute([NotNull] Func<int, TCommand> createCommand);

        [NotNull]
        protected virtual MetricsContext CreateMetricsContext()
        {
            return Metric.Context("CassandraClient");
        }

        protected void RecordTimeAndErrors([NotNull] TCommand command, [NotNull] Action<TCommand, ICommandMetrics> action)
        {
            var metrics = settings.EnableMetrics
                              ? new CommandMetrics(CreateMetricsContext(), command)
                              : CommandMetricsStub.Instance;
            using(metrics.NewTotalContext())
            {
                try
                {
                    action(command, metrics);
                }
                catch(Exception e)
                {
                    metrics.RecordError(e);
                    throw;
                }
            }
        }

        protected void TryExecuteCommandInPool([NotNull] TCommand command, [NotNull] ICommandMetrics metrics)
        {
            IThriftConnection connectionInPool = null;
            try
            {
                using(metrics.NewAcquireConnectionFromPoolContext())
                    connectionInPool = connectionPool.Acquire(command.CommandContext.KeyspaceName);
                using(metrics.NewThriftQueryContext())
                    connectionInPool.ExecuteCommand(command);
                connectionPool.Good(connectionInPool);
                connectionPool.Release(connectionInPool);
            }
            catch(Exception e)
            {
                throw HandleCommandExecutionException(e, command, connectionInPool, 0);
            }
        }

        [NotNull]
        private CassandraClientException HandleCommandExecutionException([NotNull] Exception e, [NotNull] ICommand command, [CanBeNull] IThriftConnection connectionInPool, int attempt)
        {
            var message = string.Format("An error occurred while executing cassandra command '{0}'", command.Name);
            var exception = CassandraExceptionTransformer.Transform(e, message);
            if(connectionInPool != null)
                logger.Warn(string.Format("Attempt {0} on {1} failed.", attempt, connectionInPool), exception);
            else
                logger.Warn(string.Format("Attempt {0} to all nodes failed.", attempt), exception);
            if(connectionInPool != null)
            {
                if(exception.ReduceReplicaLive)
                    connectionPool.Bad(connectionInPool);
                else
                    connectionPool.Good(connectionInPool);

                if(exception.IsCorruptConnection)
                    connectionPool.Remove(connectionInPool);
                else
                    connectionPool.Release(connectionInPool);
            }
            return exception;
        }

        public virtual void Dispose()
        {
            if(!disposed)
            {
                lock(disposeLock)
                {
                    if(!disposed)
                    {
                        disposed = true;
                        connectionPool.Dispose();
                    }
                }
            }
        }

        protected readonly ICassandraClusterSettings settings;

        private volatile bool disposed;
        private readonly object disposeLock = new object();
        private readonly ILog logger;
        private readonly IThriftConnectionReplicaSetPool connectionPool;
    }
}