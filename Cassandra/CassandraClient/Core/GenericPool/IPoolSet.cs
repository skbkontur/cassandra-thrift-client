using System;

using JetBrains.Annotations;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    internal interface IPoolSet<TItem, in TItemKey> : IDisposable
        where TItem : class, IDisposable, ILiveness
    {
        [NotNull]
        TItem Acquire(TItemKey itemKey);

        void Release([NotNull] TItem item);
        void Remove([NotNull] TItem item);

        void Bad([NotNull] TItem key);
        void Good([NotNull] TItem key);
    }
}