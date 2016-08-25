using System;

using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal static class CommandMetricsFactory
    {
        [NotNull]
        public static ICommandMetrics Create([NotNull] ICassandraClusterSettings settings, [NotNull] IFierceCommand command)
        {
            if(!settings.EnableMetrics)
                return CommandMetricsStub.Instance;

            var metricsContext = GetMetricsContext(command, globalContext.Context("Fierce"));
            return new CommandMetrics(metricsContext);
        }

        [NotNull]
        public static IRetryableCommandMetrics Create([NotNull] ICassandraClusterSettings settings, [NotNull] ISimpleCommand command)
        {
            if(!settings.EnableMetrics)
                return CommandMetricsStub.Instance;

            var metricsContext = GetMetricsContext(command, globalContext.Context("Simple"));
            return new RetryableCommandMetrics(metricsContext, FormatSinglePartitionKey(command));
        }

        [CanBeNull]
        private static string FormatSinglePartitionKey([NotNull] ICommand command)
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
        private static MetricsContext GetMetricsContext([NotNull] ICommand command, MetricsContext metricsContext)
        {
            var commandContext = command.CommandContext;
            if(!string.IsNullOrEmpty(commandContext.KeyspaceName))
                metricsContext = metricsContext.Context(commandContext.KeyspaceName);
            if(!string.IsNullOrEmpty(commandContext.ColumnFamilyName))
                metricsContext = metricsContext.Context(commandContext.ColumnFamilyName);
            return metricsContext.Context(command.Name);
        }

        private static readonly MetricsContext globalContext = Metric.Context("CassandraClient");
    }
}