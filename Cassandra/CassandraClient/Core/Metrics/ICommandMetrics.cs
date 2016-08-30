using System;

using JetBrains.Annotations;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
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