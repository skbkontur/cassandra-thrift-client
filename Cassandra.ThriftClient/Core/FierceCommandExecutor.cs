using System;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool;
using SkbKontur.Cassandra.ThriftClient.Core.Metrics;

namespace SkbKontur.Cassandra.ThriftClient.Core
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