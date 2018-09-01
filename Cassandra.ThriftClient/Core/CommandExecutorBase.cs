using System;

using JetBrains.Annotations;

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
        protected CommandExecutorBase([NotNull] IThriftConnectionReplicaSetPool connectionPool, [NotNull] ICassandraClusterSettings settings)
        {
            this.connectionPool = connectionPool;
            this.settings = settings;
        }

        public void Execute([NotNull] TCommand command)
        {
            Execute(attempt => command);
        }

        public abstract void Execute([NotNull] Func<int, TCommand> createCommand);

        protected void ExecuteCommand([NotNull] TCommand command, [NotNull] ICommandMetrics metrics)
        {
            IThriftConnection connectionInPool;
            using (metrics.NewAcquireConnectionFromPoolContext())
                connectionInPool = connectionPool.Acquire(command.CommandContext.KeyspaceName);
            ExecuteCommandInPool(connectionInPool, command, metrics);
            connectionPool.Good(connectionInPool);
            connectionPool.Release(connectionInPool);
        }

        private void ExecuteCommandInPool([NotNull] IThriftConnection connectionInPool, [NotNull] TCommand command, [NotNull] ICommandMetrics metrics)
        {
            try
            {
                using (metrics.NewThriftQueryContext())
                    connectionInPool.ExecuteCommand(command);
            }
            catch (Exception e)
            {
                var exception = CassandraExceptionTransformer.Transform(e, string.Format("Failed to execute cassandra command {0} in pool {1}", command.Name, connectionInPool));
                if (exception.ReduceReplicaLive)
                    connectionPool.Bad(connectionInPool);
                else
                    connectionPool.Good(connectionInPool);
                if (exception.IsCorruptConnection)
                    connectionPool.Remove(connectionInPool);
                else
                    connectionPool.Release(connectionInPool);
                throw exception;
            }
        }

        public virtual void Dispose()
        {
            if (!disposed)
            {
                lock (disposeLock)
                {
                    if (!disposed)
                    {
                        disposed = true;
                        connectionPool.Dispose();
                    }
                }
            }
        }

        private volatile bool disposed;
        private readonly object disposeLock = new object();
        private readonly IThriftConnectionReplicaSetPool connectionPool;
        protected readonly ICassandraClusterSettings settings;
    }
}