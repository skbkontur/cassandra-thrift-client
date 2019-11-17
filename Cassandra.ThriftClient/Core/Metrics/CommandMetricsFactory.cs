using System;

using JetBrains.Annotations;

using Metrics;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Helpers;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal static class CommandMetricsFactory
    {
        [NotNull]
        public static IFierceCommandMetrics GetMetrics([NotNull] this IFierceCommand command, [NotNull] ICassandraClusterSettings settings)
        {
            return !settings.EnableMetrics
                       ? (IFierceCommandMetrics)NoOpMetrics.Instance
                       : new FierceCommandMetrics(GetMetricsContext(command, "Fierce"));
        }

        [NotNull]
        public static ISimpleCommandMetrics GetMetrics([NotNull] this ISimpleCommand command, [NotNull] ICassandraClusterSettings settings)
        {
            return !settings.EnableMetrics
                       ? (ISimpleCommandMetrics)NoOpMetrics.Instance
                       : new SimpleCommandMetrics(GetMetricsContext(command, "Simple"), FormatSinglePartitionKey(command));
        }

        [CanBeNull]
        private static string FormatSinglePartitionKey([NotNull] ICommand command)
        {
            var singlePartitionQuery = command as ISinglePartitionQuery;
            if (singlePartitionQuery == null)
                return null;
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
        private static MetricsContext GetMetricsContext([NotNull] ICommand command, [NotNull] string commandCategory)
        {
            var metricsContext = Metric.Context("CassandraClient").Context(commandCategory);
            if (!string.IsNullOrEmpty(command.CommandContext.KeyspaceName))
                metricsContext = metricsContext.Context(command.CommandContext.KeyspaceName);
            if (!string.IsNullOrEmpty(command.CommandContext.ColumnFamilyName))
                metricsContext = metricsContext.Context(command.CommandContext.ColumnFamilyName);
            return metricsContext.Context(command.Name);
        }
    }
}