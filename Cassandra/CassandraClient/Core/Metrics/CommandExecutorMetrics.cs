using Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    public class CommandExecutorMetrics
    {
        public CommandExecutorMetrics(MetricsContext metricsContext)
        {
            Total = metricsContext.Timer("Total", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            Attempts = metricsContext.Histogram("Attempts", Unit.Items, SamplingType.FavourRecent);
            Errors = metricsContext.Meter("Errors", Unit.Requests, TimeUnit.Minutes);
            QueriedPartitions = metricsContext.Meter("QueriedPartitions", Unit.Items, TimeUnit.Minutes);
        }

        public Timer Total { get; private set; }
        public Histogram Attempts { get; private set; }
        public Meter Errors { get; private set; }
        public Meter QueriedPartitions { get; private set; }
    }
}