using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    public class MultiPool<TItem, TKey>
        where TKey : IEquatable<TKey>
        where TItem : class, IDisposable, IPoolKeyContainer<TKey>, ILiveness
    {
        public MultiPool(Func<TKey, Pool<TItem>> poolFactory)
        {
            this.poolFactory = poolFactory;
        }

        public TItem Acquire()
        {
            var poolWithHealths = pools.Values.ToArray();

            if (poolWithHealths.Length == 0)
                throw new EmptyPoolException("Cannot acquire items from empty pool. Use RegisterKey method to fill pool");

            TItem result = null;

            var pool = poolWithHealths
                .ShuffleByHealth(x => x.Health.Value, x => x.Pool)
                .FirstOrDefault(x => x.TryAcquireExists(out result));

            if(pool == null)
            {
                return poolWithHealths
                    .RandomItemByHealth(x => x.Health.Value, x => x.Pool)
                    .AcquireNew();
            }

            return result;
        }

        public void Release(TItem item)
        {
            GetPool(item.PoolKey).Release(item);
        }

        public void RegisterKey(TKey key)
        {
            pools.GetOrAdd(key, k => new PoolWithHealth
                {
                    Pool = poolFactory(k),
                    Health = new Health {Value = 1.0}
                });
        }

        public void Bad(TKey key)
        {
            PoolWithHealth poolInfo;
            if(pools.TryGetValue(key, out poolInfo))
            {
                var health = poolInfo.Health;
                var healthValue = health.Value * dieRate;
                if(healthValue < deadHealth) healthValue = deadHealth;
                health.Value = healthValue;
            }
        }

        public void Good(TKey key)
        {
            PoolWithHealth poolInfo;
            if(pools.TryGetValue(key, out poolInfo))
            {
                var health = poolInfo.Health;
                var healthValue = health.Value * aliveRate;
                if(healthValue > aliveHealth) healthValue = aliveHealth;
                health.Value = healthValue;
            }
        }

        private Pool<TItem> GetPool(TKey key)
        {
            PoolWithHealth result;
            if(!pools.TryGetValue(key, out result))
                throw new InvalidPoolKeyException(string.Format("Try to get pool for not registered key {0}", key));
            return result.Pool;
        }

        private readonly Func<TKey, Pool<TItem>> poolFactory;
        private readonly ConcurrentDictionary<TKey, PoolWithHealth> pools = new ConcurrentDictionary<TKey, PoolWithHealth>(EqualityComparer<TKey>.Default);

        private class PoolWithHealth
        {
            public Pool<TItem> Pool { get; set; }
            public Health Health { get; set; }
        }

        private const double aliveHealth = 1.0;
        private const double deadHealth = 0.01;
        private const double aliveRate = 1.5;
        private const double dieRate = 0.7;
    }
}