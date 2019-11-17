using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal interface ICommandMetrics
    {
        [NotNull]
        IDisposable NewTotalContext();

        [NotNull]
        IDisposable NewAcquireConnectionFromPoolContext();

        [NotNull]
        IDisposable NewThriftQueryContext();

        void RecordError([NotNull] Exception error);
    }
}