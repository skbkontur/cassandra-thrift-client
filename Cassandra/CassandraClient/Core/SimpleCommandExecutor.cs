using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Metrics;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class SimpleCommandExecutor : CommandExecutorBase<ISimpleCommand>
    {
        public SimpleCommandExecutor([NotNull] IPoolSet<IThriftConnection, string> connectionPool, [NotNull] ICassandraClusterSettings settings)
            : base(connectionPool, settings)
        {
        }

        public override void Execute([NotNull] ISimpleCommand command)
        {
            Execute(attempt => command);
        }

        public override void Execute([NotNull] Func<int, ISimpleCommand> createCommand)
        {
            var command = createCommand(0);
            var metrics = CommandMetricsFactory.Create(settings, command);
            using(metrics.NewTotalContext())
            {
                try
                {
                    for(var attempt = 1; attempt <= settings.Attempts; ++attempt)
                    {
                        try
                        {
                            TryExecuteCommandInPool(command, metrics, attempt-1);
                            metrics.RecordQueriedPartitions(command);
                            return;
                        }
                        catch(CassandraClientException exception)
                        {
                            if(!exception.UseAttempts)
                                throw;
                            if(attempt == 1)
                                metrics.RecordRetriedCommand();
                            if(attempt == settings.Attempts)
                                throw new CassandraAttemptsException(settings.Attempts, exception);
                            command = createCommand(attempt);
                        }
                    }
                }
                catch(Exception e)
                {
                    metrics.RecordError(e);
                    throw;
                }
            }
        }
    }
}