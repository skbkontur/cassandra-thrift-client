using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class CommandExecutorMetrics
    {
        public CommandExecutorMetrics(MetricsContext metricsContext, ICommand command)
        {
            var context = GetMetricsContext(metricsContext, command);
            CommandInfo = GetCommandInfo(command);

            Total = context.Timer("Total", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            AcquirePoolConnection = context.Timer("AcquirePoolConnection", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            ThriftQuery = context.Timer("ThriftQuery", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            Attempts = context.Histogram("Attempts", Unit.Items, SamplingType.FavourRecent);
            Errors = context.Meter("Errors", Unit.Requests, TimeUnit.Minutes);
            QueriedPartitions = context.Meter("QueriedPartitions", Unit.Items, TimeUnit.Minutes);
        }

        public Timer Total { get; private set; }
        public Timer AcquirePoolConnection { get; private set; }
        public Timer ThriftQuery { get; private set; }
        public Histogram Attempts { get; private set; }
        public Meter Errors { get; private set; }
        public Meter QueriedPartitions { get; private set; }

        public string CommandInfo { get; private set; }

        private string GetCommandInfo(ICommand command)
        {
            var singlePartitionQuery = command as ISinglePartitionQuery;
            return singlePartitionQuery == null ? null : singlePartitionQuery.PartitionKey;
        }

        private static MetricsContext GetMetricsContext(MetricsContext metricsContext, ICommand command)
        {
            var commandContext = command.CommandContext;
            if(!string.IsNullOrEmpty(commandContext.KeyspaceName))
                metricsContext = metricsContext.Context(commandContext.KeyspaceName);
            if(!string.IsNullOrEmpty(commandContext.ColumnFamilyName))
                metricsContext = metricsContext.Context(commandContext.ColumnFamilyName);
            return metricsContext.Context(command.Name);
        }
    }
}