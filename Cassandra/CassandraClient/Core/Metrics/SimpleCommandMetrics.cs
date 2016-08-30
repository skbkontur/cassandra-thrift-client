using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class SimpleCommandMetrics : CommandMetricsBase, ISimpleCommandMetrics
    {
        public SimpleCommandMetrics([NotNull] MetricsContext context, [CanBeNull] string singlePartitionKey)
            : base(context, singlePartitionKey)
        {
            retries = context.Meter("Retries", Unit.Items, TimeUnit.Minutes);
            queriedPartitions = context.Meter("QueriedPartitions", Unit.Items, TimeUnit.Minutes);
        }

        public void RecordRetry()
        {
            retries.Mark();
        }

        public void RecordQueriedPartitions([NotNull] ISimpleCommand command)
        {
            queriedPartitions.Mark(command.QueriedPartitionsCount);
        }

        private readonly Meter retries;
        private readonly Meter queriedPartitions;
    }
}