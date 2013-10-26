using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public interface IPoolSet<TItem, in TItemKey> : IDisposable
        where TItem : class, IDisposable, ILiveness 
    {
        TItem Acquire(TItemKey itemKey);
        void Release(TItem item);
        
        void Bad(TItem key);
        void Good(TItem key);
    }
}