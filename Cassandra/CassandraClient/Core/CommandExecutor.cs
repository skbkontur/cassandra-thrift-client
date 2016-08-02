using System;
using System.Collections.Concurrent;
using System.Diagnostics;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Metrics;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class CommandExecutor : CommandExecutorBase<ICommand>
    {
        public CommandExecutor(IPoolSet<IThriftConnection, string> connectionPool, ICassandraClusterSettings settings)
            : base(connectionPool, settings)
        {
        }

        protected override MetricsContext CreateMetricsContext()
        {
            return base.CreateMetricsContext().Context("Simple");
        }

        public override void Execute(ICommand command)
        {
            Execute(attempt => command);
        }

        public override void Execute(Func<int, ICommand> createCommand)
        {
            var stopwatch = Stopwatch.StartNew();

            var command = createCommand(0);
            var metrics = GetMetrics(command);
            try
            {
                using(metrics.CreateTimerContext(m => m.Total))
                {
                    for(var attempt = 1; attempt <= settings.Attempts; ++attempt)
                    {
                        try
                        {
                            TryExecuteCommandInPool(command, metrics);
                            metrics.Record(m => m.QueriedPartitions.Mark(command.QueriedPartitionsCount));
                            metrics.UpdateHistogram(m => m.Attempts, attempt);
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
                }
            }
            catch
            {
                metrics.Record(m => m.Errors.Mark());
                throw;
            }
            finally
            {
                var timeStatisticsTitle = string.Format("Cassandra.{0}{1}", command.Name, command.CommandContext);
                var timeStatistics = timeStatisticsDictionary.GetOrAdd(timeStatisticsTitle, x => new TimeStatistics(timeStatisticsTitle));
                timeStatistics.AddTime(stopwatch.ElapsedMilliseconds);
            }
        }

        private readonly ConcurrentDictionary<string, TimeStatistics> timeStatisticsDictionary = new ConcurrentDictionary<string, TimeStatistics>();
    }
}