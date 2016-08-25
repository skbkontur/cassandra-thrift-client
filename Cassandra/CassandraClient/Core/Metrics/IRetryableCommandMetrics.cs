using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal interface IRetryableCommandMetrics : ICommandMetrics
    {
        void RecordRetriedCommand();
        void RecordQueriedPartitions(ISimpleCommand command);
    }
}