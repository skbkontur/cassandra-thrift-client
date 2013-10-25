using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    internal class ReplicaSetPool<TItem, TItemKey, TReplicaKey> : IPoolSet<TItem, TItemKey>
        where TItem : class, IDisposable, ILiveness
    {
        public ReplicaSetPool(Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
                              IEqualityComparer<TReplicaKey> replicaKeyComparer,
                              IEqualityComparer<TItemKey> itemKeyComparer,
                              Func<TItem, TReplicaKey> getReplicaKeyByItem,
                              Func<TItem, TItemKey> getItemKeyByItem)
        {
            this.poolFactory = poolFactory;
            this.getReplicaKeyByItem = getReplicaKeyByItem;
            this.getItemKeyByItem = getItemKeyByItem;
            replicaHealth = new ConcurrentDictionary<TReplicaKey, Health>(replicaKeyComparer);
            pools = new ConcurrentDictionary<PoolKey, Pool<TItem>>(new PoolKeyEqualityComparer(replicaKeyComparer, itemKeyComparer));
        }

        public void Dispose()
        {
            pools.Values.ToList().ForEach(p => p.Dispose());
        }

        public TItem Acquire(TItemKey itemKey)
        {
            var replicaHealths = replicaHealth.ToArray();

            if(replicaHealths.Length == 0)
                throw new EmptyPoolException("Cannot acquire items without configured replicas. Use RegisterReplica method to fill pool");

            TItem result = null;

            var pool = replicaHealths
                .ShuffleByHealth(x => x.Value.Value, x => GetPool(itemKey, x.Key))
                .FirstOrDefault(x => x.TryAcquireExists(out result));

            if(pool == null)
            {
                var newItems = replicaHealths
                    .ShuffleByHealth(x => x.Value.Value, x => GetPool(itemKey, x.Key))
                    .Select(x => x.AcquireNew());

                foreach(var newItem in newItems)
                {
                    if(!newItem.IsAlive)
                    {
                        Bad(newItem);
                        continue;
                    }
                    return newItem;
                }
                throw new AllItemsIsDeadExceptions(string.Format("Cannot acquire connection from any of pool with keys [{0}]", string.Join(", ", pools.Keys.Select(x => x.ToString()))));
            }

            return result;
        }

        public void Release(TItem item)
        {
            GetPool(getItemKeyByItem(item), getReplicaKeyByItem(item), false).Release(item);
        }

        public void RegisterReplica(TReplicaKey key)
        {
            replicaHealth.GetOrAdd(key, k => new Health {Value = 1.0});
        }

        public void Bad(TItem item)
        {
            BadReplica(getReplicaKeyByItem(item));
        }

        public void Good(TItem item)
        {
            GoodReplica(getReplicaKeyByItem(item));
        }

        internal void BadReplica(TReplicaKey replicaKey)
        {
            Health health;
            if(replicaHealth.TryGetValue(replicaKey, out health))
            {
                var healthValue = health.Value * dieRate;
                if(healthValue < deadHealth) healthValue = deadHealth;
                health.Value = healthValue;
            }
        }

        private Pool<TItem> GetPool(TItemKey itemKey, TReplicaKey replicaKey, bool createNewIfNotExists = true)
        {
            var key = new PoolKey(itemKey, replicaKey);
            if(!createNewIfNotExists)
            {
                Pool<TItem> result;
                if(!pools.TryGetValue(key, out result))
                    throw new InvalidPoolKeyException(string.Format("Pool with key [{0}] does not exists", key));
                return result;
            }
            return pools.GetOrAdd(key, x => poolFactory(x.ItemKey, x.ReplicaKey));
        }

        private void GoodReplica(TReplicaKey replicaKey)
        {
            Health health;
            if(replicaHealth.TryGetValue(replicaKey, out health))
            {
                var healthValue = health.Value * aliveRate;
                if(healthValue > aliveHealth) healthValue = aliveHealth;
                health.Value = healthValue;
            }
        }

        private readonly Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory;
        private readonly Func<TItem, TReplicaKey> getReplicaKeyByItem;
        private readonly Func<TItem, TItemKey> getItemKeyByItem;
        private readonly ConcurrentDictionary<PoolKey, Pool<TItem>> pools;
        private readonly ConcurrentDictionary<TReplicaKey, Health> replicaHealth;

        private class PoolKey
        {
            public PoolKey(TItemKey itemKey, TReplicaKey replicaKey)
            {
                ItemKey = itemKey;
                ReplicaKey = replicaKey;
            }

            public TItemKey ItemKey { get; private set; }
            public TReplicaKey ReplicaKey { get; private set; }
        }

        private class PoolKeyEqualityComparer : IEqualityComparer<PoolKey>
        {
            public PoolKeyEqualityComparer(IEqualityComparer<TReplicaKey> replicaKeyComparer, IEqualityComparer<TItemKey> itemKeyComparer)
            {
                this.replicaKeyComparer = replicaKeyComparer;
                this.itemKeyComparer = itemKeyComparer;
            }

            public bool Equals(PoolKey x, PoolKey y)
            {
                if(ReferenceEquals(x, y)) return true;
                if(ReferenceEquals(null, x) || ReferenceEquals(null, y)) return false;
                return
                    itemKeyComparer.Equals(x.ItemKey, y.ItemKey) &&
                    replicaKeyComparer.Equals(x.ReplicaKey, y.ReplicaKey);
            }

            public int GetHashCode(PoolKey obj)
            {
                unchecked
                {
                    return (itemKeyComparer.GetHashCode(obj.ItemKey) * 397) ^ replicaKeyComparer.GetHashCode(obj.ReplicaKey);
                }
            }

            private readonly IEqualityComparer<TReplicaKey> replicaKeyComparer;
            private readonly IEqualityComparer<TItemKey> itemKeyComparer;
        }

        private const double aliveHealth = 1.0;
        private const double deadHealth = 0.01;
        private const double aliveRate = 1.5;
        private const double dieRate = 0.7;
    }

    internal static class ReplicaSetPool
    {
        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory)
            where TItem : class, IDisposable, IPoolKeyContainer<TItemKey, TReplicaKey>, ILiveness
            where TItemKey : IEquatable<TItemKey>
            where TReplicaKey : IEquatable<TReplicaKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, i => i.ReplicaKey, i => i.PoolKey);
        }

        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(
            Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
            Func<TItem, TReplicaKey> getReplicaKeyByItem,
            Func<TItem, TItemKey> getItemKeyByItem)
            where TItem : class, IDisposable, ILiveness
            where TItemKey : IEquatable<TItemKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, getReplicaKeyByItem, getItemKeyByItem);
        }
    }
}