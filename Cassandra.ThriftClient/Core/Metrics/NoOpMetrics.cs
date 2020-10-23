﻿using System;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Core.Metrics
{
    internal sealed class NoOpMetrics : ISimpleCommandMetrics, IFierceCommandMetrics, IConnectionPoolMetrics
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
        public IDisposable AcquireNewConnectionContext()
        {
            return NoOpContext.Instance;
        }

        [NotNull]
        public static readonly NoOpMetrics Instance = new NoOpMetrics();
    }
}