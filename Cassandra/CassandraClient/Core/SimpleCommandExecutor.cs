using System;

using JetBrains.Annotations;

using log4net;

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
            if(settings.Attempts <= 0)
                throw new InvalidOperationException(string.Format("settings.Attempts <= 0 for: {0}", settings));
        }

        public override sealed void Execute([NotNull] Func<int, ISimpleCommand> createCommand)
        {
            var attempt = 0;
            var command = createCommand(attempt);
            var metrics = command.GetMetrics(settings);
            using(metrics.NewTotalContext())
            {
                while(true)
                {
                    try
                    {
                        ExecuteCommand(command, metrics);
                        metrics.RecordQueriedPartitions(command);
                        break;
                    }
                    catch(CassandraClientException exception)
                    {
                        metrics.RecordError(exception);
                        logger.Warn(string.Format("Attempt {0} failed", attempt), exception);
                        if(!exception.UseAttempts)
                            throw;
                        if(attempt == 0)
                            metrics.RecordRetry();
                        if(++attempt == settings.Attempts)
                            throw new CassandraAttemptsException(settings.Attempts, exception);
                        command = createCommand(attempt);
                    }
                }
            }
        }

        private readonly ILog logger = LogManager.GetLogger(typeof(SimpleCommandExecutor));
    }
}