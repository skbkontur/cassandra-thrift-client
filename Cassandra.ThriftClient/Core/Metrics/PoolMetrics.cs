using JetBrains.Annotations;

using Metrics;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal class PoolMetrics : IPoolMetrics
    {
        public PoolMetrics([NotNull] MetricsContext context)
        {
            newConnections = context.Meter("AcquireNewConnection", Unit.Items, TimeUnit.Minutes);
        }

        public void RecordAcquireNewConnection()
        {
            newConnections.Mark();
        }

        private readonly Meter newConnections;
    }
}