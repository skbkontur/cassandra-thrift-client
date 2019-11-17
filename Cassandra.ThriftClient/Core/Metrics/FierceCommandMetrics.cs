using JetBrains.Annotations;

using Metrics;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal class FierceCommandMetrics : CommandMetricsBase, IFierceCommandMetrics
    {
        public FierceCommandMetrics([NotNull] MetricsContext context)
            : base(context, singlePartitionKey : null)
        {
        }
    }
}