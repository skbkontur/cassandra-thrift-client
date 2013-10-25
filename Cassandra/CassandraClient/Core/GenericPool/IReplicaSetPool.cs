using System;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public interface IReplicaSetPool<TItem, in TItemKey, in TReplicaKey> : IDisposable
        where TItem : class, IDisposable, ILiveness 
    {
        TItem Acquire(TItemKey itemKey);

        void Release(TItem item);
        
        void RegisterKey(TReplicaKey key);
        void Bad(TItem key);
        void Good(TItem key);
    }
}