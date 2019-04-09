using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal sealed class NoOpMetrics : ISimpleCommandMetrics, IFierceCommandMetrics
    {
        private NoOpMetrics()
        {
        }

        [NotNull]
        public IDisposable NewTotalContext()
        {
            return NoOpContext.Instance;
        }

        [NotNull]
        public IDisposable NewAcquireConnectionFromPoolContext()
        {
            return NoOpContext.Instance;
        }

        [NotNull]
        public IDisposable NewThriftQueryContext()
        {
            return NoOpContext.Instance;
        }

        public void RecordError([NotNull] Exception error)
        {
        }

        public void RecordRetry()
        {
        }

        public void RecordCommandExecutionInfo([NotNull] ISimpleCommand command)
        {
        }

        [NotNull]
        public static readonly NoOpMetrics Instance = new NoOpMetrics();
    }
}