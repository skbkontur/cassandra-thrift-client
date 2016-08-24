using System;

using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class CommandExecutorMetrics : ICommandExecutorMetrics
    {
        public CommandExecutorMetrics([NotNull] MetricsContext metricsContext, [NotNull] ICommand command)
        {
            var context = GetMetricsContext(metricsContext, command);
            commandInfo = GetCommandInfo(command);

            total = context.Timer("Total", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            acquirePoolConnection = context.Timer("AcquirePoolConnection", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            thriftQuery = context.Timer("ThriftQuery", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            attempts = context.Histogram("Attempts", Unit.Items, SamplingType.FavourRecent);
            errors = context.Meter("Errors", Unit.Requests, TimeUnit.Minutes);
            queriedPartitions = context.Meter("QueriedPartitions", Unit.Items, TimeUnit.Minutes);
        }

        [CanBeNull]
        private string GetCommandInfo([NotNull] ICommand command)
        {
            var singlePartitionQuery = command as ISinglePartitionQuery;
            if(singlePartitionQuery == null) return null;
            try
            {
                return StringExtensions.BytesToString(singlePartitionQuery.PartitionKey);
            }
            catch
            {
                return BitConverter.ToString(singlePartitionQuery.PartitionKey);
            }
        }

        [NotNull]
        private static MetricsContext GetMetricsContext([NotNull] MetricsContext metricsContext, [NotNull] ICommand command)
        {
            var commandContext = command.CommandContext;
            if(!string.IsNullOrEmpty(commandContext.KeyspaceName))
                metricsContext = metricsContext.Context(commandContext.KeyspaceName);
            if(!string.IsNullOrEmpty(commandContext.ColumnFamilyName))
                metricsContext = metricsContext.Context(commandContext.ColumnFamilyName);
            return metricsContext.Context(command.Name);
        }

        [NotNull]
        public IDisposable TotalTimeContext { get { return total.NewContext(commandInfo); } }

        [NotNull]
        public IDisposable AcquirePoolConnectionContext { get { return acquirePoolConnection.NewContext(commandInfo); } }

        [NotNull]
        public IDisposable ThriftQueryContext { get { return thriftQuery.NewContext(commandInfo); } }

        public void RecordAttempts(long attemptsCount)
        {
            attempts.Update(attemptsCount, commandInfo);
        }

        public void RecordError()
        {
            errors.Mark();
        }

        public void RecordQueriedPartitions(ISimpleCommand command)
        {
            queriedPartitions.Mark(command.QueriedPartitionsCount);
        }

        private readonly Timer total;
        private readonly Timer acquirePoolConnection;
        private readonly Timer thriftQuery;
        private readonly Histogram attempts;
        private readonly Meter errors;
        private readonly Meter queriedPartitions;
        private readonly string commandInfo;
    }
}