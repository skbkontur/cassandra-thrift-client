using System;

using JetBrains.Annotations;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal sealed class NoOpContext : IDisposable
    {
        private NoOpContext()
        {
        }

        public void Dispose()
        {
        }

        [NotNull]
        public static readonly NoOpContext Instance = new NoOpContext();
    }
}