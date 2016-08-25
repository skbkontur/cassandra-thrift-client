using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal sealed class CommandMetricsStub : ICommandMetrics
    {
        private CommandMetricsStub()
        {
        }

        [NotNull]
        public IDisposable NewTotalContext()
        {
            return DisposableStub.StubInstance;
        }

        [NotNull]
        public IDisposable NewAcquireConnectionFromPoolContext()
        {
            return DisposableStub.StubInstance;
        }

        [NotNull]
        public IDisposable NewThriftQueryContext()
        {
            return DisposableStub.StubInstance;
        }

        public void RecordRetriedCommand()
        {
        }

        public void RecordError(Exception error)
        {
        }

        public void RecordQueriedPartitions(ISimpleCommand command)
        {
        }

        public static readonly ICommandMetrics Instance = new CommandMetricsStub();

        private sealed class DisposableStub : IDisposable
        {
            private DisposableStub()
            {
            }

            public void Dispose()
            {
            }

            public static readonly DisposableStub StubInstance = new DisposableStub();
        }
    }
}