using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class FierceCommandExecutor : CommandExecutorBase<IFierceCommand>
    {
        public FierceCommandExecutor([NotNull] IPoolSet<IThriftConnection, string> connectionPool, [NotNull] ICassandraClusterSettings settings)
            : base(connectionPool, settings)
        {
        }

        public override sealed void Execute([NotNull] Func<int, IFierceCommand> createCommand)
        {
            var command = createCommand(0);
            var metrics = command.GetMetrics(settings);
            using (metrics.NewTotalContext())
            {
                try
                {
                    ExecuteCommand(command, metrics);
                }
                catch (Exception e)
                {
                    metrics.RecordError(e);
                    throw;
                }
            }
        }
    }
}