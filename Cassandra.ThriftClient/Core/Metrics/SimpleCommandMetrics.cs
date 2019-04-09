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
            responseSize = context.Meter("ResponseSize", Unit.Bytes, TimeUnit.Minutes);
        }

        public void RecordRetry()
        {
            retries.Mark();
        }

        public void RecordCommandExecutionInfo(ISimpleCommand command)
        {
            queriedPartitions.Mark(command.QueriedPartitionsCount);
            if(command.ResponseSize.HasValue)
                responseSize.Mark(command.ResponseSize.Value);
        }

        private readonly Meter retries;
        private readonly Meter queriedPartitions;
        private readonly Meter responseSize;
    }
}