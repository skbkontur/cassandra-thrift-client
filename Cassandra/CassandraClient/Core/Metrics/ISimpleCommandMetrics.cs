using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal interface ISimpleCommandMetrics : ICommandMetrics
    {
        void RecordRetry();
        void RecordQueriedPartitions([NotNull] ISimpleCommand command);
    }
}