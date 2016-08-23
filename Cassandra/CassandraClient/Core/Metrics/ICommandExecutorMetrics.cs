using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal interface ICommandExecutorMetrics
    {
        [NotNull]
        IDisposable TotalTimeContext { get; }

        [NotNull]
        IDisposable AcquirePoolConnectionContext { get; }

        [NotNull]
        IDisposable ThriftQueryContext { get; }

        void RecordAttempts(long attemptsCount);
        void RecordError();
        void RecordQueriedPartitions(ISimpleCommand command);
    }
}