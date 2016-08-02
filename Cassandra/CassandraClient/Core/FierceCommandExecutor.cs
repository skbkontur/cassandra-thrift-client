using System;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class FierceCommandExecutor : CommandExecutorBase<IFierceCommand>
    {
        public FierceCommandExecutor(IPoolSet<IThriftConnection, string> connectionPool, ICassandraClusterSettings settings)
            : base(connectionPool, settings)
        {
        }

        protected override MetricsContext CreateMetricsContext()
        {
            return base.CreateMetricsContext().Context("Fierce");
        }

        public override void Execute(IFierceCommand command)
        {
            var metrics = GetMetrics(command);
            using(metrics.CreateTimerContext(m => m.Total))
            {
                try
                {
                    TryExecuteCommandInPool(command, metrics);
                }
                catch
                {
                    metrics.Record(m => m.Errors.Mark());
                    throw;
                }
            }
        }

        public override void Execute(Func<int, IFierceCommand> createCommand)
        {
            Execute(createCommand(0));
        }
    }
}