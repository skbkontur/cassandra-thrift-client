using System;

using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class SimpleCommandExecutor : CommandExecutorBase<ISimpleCommand>
    {
        public SimpleCommandExecutor([NotNull] IPoolSet<IThriftConnection, string> connectionPool, [NotNull] ICassandraClusterSettings settings)
            : base(connectionPool, settings)
        {
        }

        [NotNull]
        protected override MetricsContext CreateMetricsContext()
        {
            return base.CreateMetricsContext().Context("Simple");
        }

        public override void Execute([NotNull] ISimpleCommand command)
        {
            Execute(attempt => command);
        }

        public override void Execute([NotNull] Func<int, ISimpleCommand> createCommand)
        {
            RecordTimeAndErrors(createCommand(0), (command, metrics) =>
                {
                    for(var attempt = 1; attempt <= settings.Attempts; ++attempt)
                    {
                        try
                        {
                            TryExecuteCommandInPool(command, metrics);
                            metrics.RecordQueriedPartitions(command);
                            metrics.RecordAttempts(attempt);
                            return;
                        }
                        catch(CassandraClientException exception)
                        {
                            if(!exception.UseAttempts)
                                throw;
                            if(attempt == settings.Attempts)
                                throw new CassandraAttemptsException(settings.Attempts, exception);
                            command = createCommand(attempt);
                        }
                    }
                });
        }
    }
}