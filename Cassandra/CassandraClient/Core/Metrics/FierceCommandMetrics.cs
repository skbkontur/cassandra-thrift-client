using JetBrains.Annotations;

using Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class FierceCommandMetrics : CommandMetricsBase, IFierceCommandMetrics
    {
        public FierceCommandMetrics([NotNull] MetricsContext context)
            : base(context, singlePartitionKey : null)
        {
        }
    }
}