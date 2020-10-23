using System;

using JetBrains.Annotations;

using Metrics;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal class ConnectionPoolMetrics : IConnectionPoolMetrics
    {
        public ConnectionPoolMetrics([NotNull] MetricsContext context)
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