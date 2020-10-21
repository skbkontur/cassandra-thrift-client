using System;

using JetBrains.Annotations;

using Metrics;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal class PoolMetrics : IPoolMetrics
    {
        public PoolMetrics([NotNull] MetricsContext context)
        {
            newConnections = context.Timer("AcquireNewConnection", Unit.Items, SamplingType.ExponentiallyDecaying, TimeUnit.Minutes);
        }

        [NotNull]
        public IDisposable AcquireNewConnectionContext()
        {
            return newConnections.NewContext();
        }

        private readonly Timer newConnections;
    }
}