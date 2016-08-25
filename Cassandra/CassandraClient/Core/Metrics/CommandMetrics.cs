using System;

using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class CommandMetrics : ICommandMetrics
    {
        public CommandMetrics([NotNull] MetricsContext metricsContext, [NotNull] ICommand command)
        {
            var context = GetMetricsContext(metricsContext, command);
            singlePartitionKey = FormatSinglePartitionKey(command);

            total = context.Timer("Total", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            acquireConnectionFromPool = context.Timer("AcquireConnectionFromPool", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            thriftQuery = context.Timer("ThriftQuery", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            retriedCommands = context.Meter("RetriedCommands", Unit.Items, TimeUnit.Minutes);
            errors = context.Meter("Errors", Unit.Items, TimeUnit.Minutes);
            queriedPartitions = context.Meter("QueriedPartitions", Unit.Items, TimeUnit.Minutes);
        }

        [CanBeNull]
        private string FormatSinglePartitionKey([NotNull] ICommand command)
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
        public IDisposable NewTotalContext()
        {
            return total.NewContext(singlePartitionKey);
        }

        [NotNull]
        public IDisposable NewAcquireConnectionFromPoolContext()
        {
            return acquireConnectionFromPool.NewContext(singlePartitionKey);
        }

        [NotNull]
        public IDisposable NewThriftQueryContext()
        {
            return thriftQuery.NewContext(singlePartitionKey);
        }

        public void RecordRetriedCommand()
        {
            retriedCommands.Mark();
        }

        public void RecordError(Exception error)
        {
            errors.Mark(error.GetType().Name);
        }

        public void RecordQueriedPartitions(ISimpleCommand command)
        {
            queriedPartitions.Mark(command.QueriedPartitionsCount);
        }

        private readonly Timer total;
        private readonly Timer acquireConnectionFromPool;
        private readonly Timer thriftQuery;
        private readonly Meter retriedCommands;
        private readonly Meter errors;
        private readonly Meter queriedPartitions;
        private readonly string singlePartitionKey;
    }
}