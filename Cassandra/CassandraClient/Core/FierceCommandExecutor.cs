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

        public override void Execute([NotNull] IFierceCommand command)
        {
            var metrics = CommandMetricsFactory.Create(settings, command);
            using(metrics.NewTotalContext())
            {
                try
                {
                    TryExecuteCommandInPool(command, metrics);
                }
                catch(Exception e)
                {
                    metrics.RecordError(e);
                    throw;
                }
            }
        }

        public override void Execute([NotNull] Func<int, IFierceCommand> createCommand)
        {
            Execute(createCommand(0));
        }
    }
}