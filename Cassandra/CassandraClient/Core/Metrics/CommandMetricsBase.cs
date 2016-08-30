using System;

using JetBrains.Annotations;

using Metrics;

namespace SKBKontur.Cassandra.CassandraClient.Core.Metrics
{
    internal abstract class CommandMetricsBase : ICommandMetrics
    {
        protected CommandMetricsBase([NotNull] MetricsContext context, [CanBeNull] string singlePartitionKey)
        {
            this.singlePartitionKey = singlePartitionKey;
            total = context.Timer("Total", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            acquireConnectionFromPool = context.Timer("AcquireConnectionFromPool", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            thriftQuery = context.Timer("ThriftQuery", Unit.Requests, SamplingType.FavourRecent, TimeUnit.Minutes);
            errors = context.Meter("Errors", Unit.Items, TimeUnit.Minutes);
        }

        [NotNull]
        public IDisposable NewTotalContext()
        {
            return total.NewContext(singlePartitionKey);
        }

        [NotNull]
        public IDisposable NewAcquireConnectionFromPoolContext()
        {
            return acquireConnectionFromPool.NewContext(singlePartitionKey);
        }

        [NotNull]
        public IDisposable NewThriftQueryContext()
        {
            return thriftQuery.NewContext(singlePartitionKey);
        }

        public void RecordError([NotNull] Exception error)
        {
            errors.Mark(error.GetType().Name);
        }

        private readonly string singlePartitionKey;
        private readonly Timer total;
        private readonly Timer acquireConnectionFromPool;
        private readonly Timer thriftQuery;
        private readonly Meter errors;
    }
}