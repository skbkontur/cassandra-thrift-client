using System;

using JetBrains.Annotations;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
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