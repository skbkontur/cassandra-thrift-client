using System;

using JetBrains.Annotations;

using Metrics;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class FierceCommandExecutor : CommandExecutorBase<IFierceCommand>
    {
        public FierceCommandExecutor([NotNull] IPoolSet<IThriftConnection, string> connectionPool, [NotNull] ICassandraClusterSettings settings)
            : base(connectionPool, settings)
        {
        }

        [NotNull]
        protected override MetricsContext CreateMetricsContext()
        {
            return base.CreateMetricsContext().Context("Fierce");
        }

        public override void Execute([NotNull] IFierceCommand command)
        {
            RecordTimeAndErrors(command, TryExecuteCommandInPool);
        }

        public override void Execute([NotNull] Func<int, IFierceCommand> createCommand)
        {
            Execute(createCommand(0));
        }
    }
}