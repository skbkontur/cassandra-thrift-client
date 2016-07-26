using System;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal class CommandExecutorWithMetrics : ICommandExecuter
    {
        public CommandExecutorWithMetrics(ICommandExecuter commandExecutor)
        {
            this.commandExecutor = commandExecutor;
        }

        public void Dispose()
        {
            commandExecutor.Dispose();
        }

        public void Execute(ICommand command)
        {
            Execute(attempt => command);
        }

        public void Execute(Func<int, ICommand> createCommand)
        {
            var command = createCommand(0);
            var metrics = GetMetrics(command);
            RecordQueriedPartitions(metrics, command);
            var queriedPartitionKey = TryGetQueriedPartitionKey(command);
            using (metrics.Total.NewContext(queriedPartitionKey))
            {
                try
                {
                    var totalAttempts = 0;
                    commandExecutor.Execute(attempt =>
                        {
                            totalAttempts++;
                            return attempt == 0 ? command : createCommand(attempt);
                        });
                    metrics.Attempts.Update(totalAttempts, queriedPartitionKey);
                }
                catch(Exception)
                {
                    metrics.Errors.Mark();
                    throw;
                }
            }
        }

        private string TryGetQueriedPartitionKey(ICommand command)
        {
            var singlePartitionQuery = command as ISinglePartitionQuery;
            return singlePartitionQuery == null ? null : singlePartitionQuery.PartitionKey;
        }

        private void RecordQueriedPartitions(CommandExecutorMetrics metrics, ICommand command)
        {
            if(command is ISinglePartitionQuery)
            {
                metrics.QueriedPartitions.Mark(1);
                return;
            }
            var multiPartitionsQuery = command as IMultiPartitionsQuery;
            if(multiPartitionsQuery != null)
                metrics.QueriedPartitions.Mark(multiPartitionsQuery.QueriedPartitions);
        }

        private CommandExecutorMetrics GetMetrics(ICommand command)
        {
            var metricsContext = Metric.Context("CassandraClient");
            var commandContext = command.CommandContext;
            if(!string.IsNullOrEmpty(commandContext.KeyspaceName))
                metricsContext = metricsContext.Context(commandContext.KeyspaceName);
            if(!string.IsNullOrEmpty(commandContext.ColumnFamilyName))
                metricsContext = metricsContext.Context(commandContext.ColumnFamilyName);
            return new CommandExecutorMetrics(metricsContext.Context(command.Name));
        }

        public void ExecuteSchemeUpdateCommandOnce(ISchemeUpdateCommand command)
        {
            commandExecutor.ExecuteSchemeUpdateCommandOnce(command);
        }

        private readonly ICommandExecuter commandExecutor;
    }
}