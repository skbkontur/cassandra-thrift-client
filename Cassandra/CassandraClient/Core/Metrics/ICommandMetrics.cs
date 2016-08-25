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

        void RecordRetriedCommand();
        void RecordError(Exception error);
        void RecordQueriedPartitions(ISimpleCommand command);
    }
}