using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal interface IConnectionPoolMetrics
    {
        [NotNull]
        IDisposable AcquireNewConnectionContext();
    }
}