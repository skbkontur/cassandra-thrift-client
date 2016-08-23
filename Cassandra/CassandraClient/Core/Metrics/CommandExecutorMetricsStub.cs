using System;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal sealed class CommandExecutorMetricsStub : ICommandExecutorMetrics
    {
        private CommandExecutorMetricsStub()
        {
        }

        [NotNull]
        public IDisposable TotalTimeContext { get { return DisposableStub.StubInstance; } }

        [NotNull]
        public IDisposable AcquirePoolConnectionContext { get { return DisposableStub.StubInstance; } }

        [NotNull]
        public IDisposable ThriftQueryContext { get { return DisposableStub.StubInstance; } }

        public void RecordAttempts(long attemptsCount)
        {
        }

        public void RecordError()
        {
        }

        public void RecordQueriedPartitions(ISimpleCommand command)
        {
        }

        public static readonly ICommandExecutorMetrics Instance = new CommandExecutorMetricsStub();

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