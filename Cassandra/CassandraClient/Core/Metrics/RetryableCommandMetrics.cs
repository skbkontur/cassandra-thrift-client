using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class RetryableCommandMetrics : CommandMetrics, IRetryableCommandMetrics
    {
        public RetryableCommandMetrics([NotNull] MetricsContext context, [CanBeNull] string singlePartitionKey = null)
            : base(context, singlePartitionKey)
        {
            retriedCommands = context.Meter("RetriedCommands", Unit.Items, TimeUnit.Minutes);
            queriedPartitions = context.Meter("QueriedPartitions", Unit.Items, TimeUnit.Minutes);
        }

        public void RecordRetriedCommand()
        {
            retriedCommands.Mark();
        }

        public void RecordQueriedPartitions(ISimpleCommand command)
        {
            queriedPartitions.Mark(command.QueriedPartitionsCount);
        }

        private readonly Meter retriedCommands;
        private readonly Meter queriedPartitions;
    }
}