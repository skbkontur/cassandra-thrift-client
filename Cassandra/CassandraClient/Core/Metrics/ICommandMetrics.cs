using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

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

        void RecordAttempts(long attemptsCount);
        void RecordError();
        void RecordQueriedPartitions(ISimpleCommand command);
    }
}