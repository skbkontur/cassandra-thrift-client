using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using log4net;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Core.GenericPool
{
    internal class ReplicaSetPool<TItem, TItemKey, TReplicaKey> : IPoolSet<TItem, TItemKey>
        where TItem : class, IDisposable, ILiveness
    {
        public ReplicaSetPool(IList<TReplicaKey> replicas,
                              Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
                              IEqualityComparer<TReplicaKey> replicaKeyComparer,
                              IEqualityComparer<TItemKey> itemKeyComparer,
                              Func<TItem, TReplicaKey> getReplicaKeyByItem,
                              Func<TItem, TItemKey> getItemKeyByItem,
                              TimeSpan? itemIdleTimeout = null)
        {
            if(replicas.Count == 0)
                throw new EmptyPoolException("Cannot create empty ReplicaSetPool");
            logger.InfoFormat("ReplicaSetPool created with client topology: {0}", string.Join(", ", replicas));
            replicaIndicies = new Dictionary<TReplicaKey, int>(replicaKeyComparer);
            replicaHealths = new ReplicaHealth<TReplicaKey>[replicas.Count];
            for(var idx = 0; idx < replicas.Count; idx++)
            {
                var replica = replicas[idx];
                replicaIndicies.Add(replica, idx);
                replicaHealths[idx] = new ReplicaHealth<TReplicaKey>(replica, aliveHealth);
            }
            this.poolFactory = poolFactory;
            this.getReplicaKeyByItem = getReplicaKeyByItem;
            this.getItemKeyByItem = getItemKeyByItem;
            pools = new ConcurrentDictionary<PoolKey, Pool<TItem>>(new PoolKeyEqualityComparer(replicaKeyComparer, itemKeyComparer));

            disposeEvent = new ManualResetEvent(false);
            if(itemIdleTimeout != null)
            {
                logger.InfoFormat("Item idle timeout: {0}", itemIdleTimeout.Value);
                unusedItemsCollectorThread = new Thread(() => UnusedItemsCollectorProcedure(itemIdleTimeout.Value))
                    {
                        IsBackground = true
                    };
                unusedItemsCollectorThread.Start();
            }
        }

        public void Dispose()
        {
            disposeEvent.Set();
            if(unusedItemsCollectorThread != null)
            {
                if(!unusedItemsCollectorThread.Join(TimeSpan.FromMilliseconds(100)))
                    logger.WarnFormat("UnusedItemsCollector do not completed in 100ms. Skip waiting.");
            }
            pools.Values.ToList().ForEach(p => p.Dispose());
            disposeEvent.Dispose();
        }

        public TItem Acquire(TItemKey itemKey)
        {
            TItem result = null;

            var totalReplicaCount = GetPoolItemCountByKey(itemKey);
            var totalReplicaHealth = replicaHealths.Select(x => x.Value).Sum();

            var existingAcquired = replicaHealths
                .ShuffleByHealth(x => x.Value, x => new {Health = x.Value, x.ReplicaKey})
                .Any(poolInfo =>
                     TryAcquireExistsOrNew(
                         new PoolKey(itemKey, poolInfo.ReplicaKey),
                         GetDesiredActiveItemCountForReplica(poolInfo.Health, totalReplicaHealth, totalReplicaCount),
                         out result));

            if(!existingAcquired)
            {
                var replicaKeys = replicaHealths.ShuffleByHealth(x => x.Value, x => x.ReplicaKey);

                var exceptions = new List<Exception>();
                foreach(var replicaKey in replicaKeys)
                {
                    var replicaPool = GetPool(itemKey, replicaKey);
                    try
                    {
                        TItem item;
                        if(TryAcquireNew(replicaPool, out item))
                            return item;
                    }
                    catch(Exception e)
                    {
                        logger.Warn(string.Format("Cannot acquire new connection for replica [{0}]", replicaKey), e);
                        BadReplica(replicaKey);
                        exceptions.Add(e);
                    }
                }
                throw new AllItemsIsDeadExceptions(
                    string.Format("Cannot acquire connection from any of pool with keys [{0}]", string.Join(", ", pools.Keys.Select(x => x.ToString()))),
                    exceptions
                    );
            }

            return result;
        }

        public void Release(TItem item)
        {
            GetPool(getItemKeyByItem(item), getReplicaKeyByItem(item), false).Release(item);
        }

        public void Remove(TItem item)
        {
            GetPool(getItemKeyByItem(item), getReplicaKeyByItem(item), false).Remove(item);
        }

        public void Bad(TItem item)
        {
            BadReplica(getReplicaKeyByItem(item));
        }

        public void Good(TItem item)
        {
            GoodReplica(getReplicaKeyByItem(item));
        }

        public Dictionary<PoolKey, KeyspaceConnectionPoolKnowledge> GetActiveItemsInfo()
        {
            return pools.ToDictionary(x => x.Key, x => new KeyspaceConnectionPoolKnowledge
                {
                    FreeConnectionCount = x.Value.FreeItemCount,
                    BusyConnectionCount = x.Value.BusyItemCount
                });
        }

        public class PoolKey
        {
            public PoolKey(TItemKey itemKey, TReplicaKey replicaKey)
            {
                ItemKey = itemKey;
                ReplicaKey = replicaKey;
            }

            public override string ToString()
            {
                return string.Format("PoolKey[Key: {0}, ReplicaKey: {1}]", ItemKey, ReplicaKey);
            }

            public TItemKey ItemKey { get; private set; }
            public TReplicaKey ReplicaKey { get; private set; }
        }

        internal void BadReplica(TReplicaKey replicaKey)
        {
            int replicaIndex;
            if(replicaIndicies.TryGetValue(replicaKey, out replicaIndex))
            {
                var replicaHealth = replicaHealths[replicaIndex];
                var healthValue = replicaHealth.Value * dieRate;
                if(healthValue < deadHealth) healthValue = deadHealth;
                replicaHealth.Value = healthValue;
            }
        }

        private bool TryAcquireNew(Pool<TItem> replicaPool, out TItem result)
        {
            result = replicaPool.AcquireNew();
            if(!result.IsAlive)
            {
                Bad(result);
                result = null;
                return false;
            }
            return true;
        }

        private static double GetDesiredActiveItemCountForReplica(double replicaHealth, double totalReplicaHealth, int totalReplicaCount)
        {
            return (replicaHealth / totalReplicaHealth) * totalReplicaCount;
        }

        private bool TryAcquireExistsOrNew(PoolKey poolKey, double replicaDesiredItemCount, out TItem result)
        {
            var pool = GetPool(poolKey);
            if(pool.TryAcquireExists(out result))
                return true;

            if(pool.TotalCount <= replicaDesiredItemCount)
            {
                try
                {
                    return TryAcquireNew(pool, out result);
                }
                catch(Exception e)
                {
                    logger.Warn(string.Format("Cannot acquire new connection for replica [{0}]", poolKey.ReplicaKey), e);
                    BadReplica(poolKey.ReplicaKey);
                    return false;
                }
            }
            return false;
        }

        private void UnusedItemsCollectorProcedure(TimeSpan itemIdleTimeout)
        {
            var checkInterval = TimeSpan.FromMilliseconds(itemIdleTimeout.TotalMilliseconds / 2);
            while(true)
            {
                if(disposeEvent.WaitOne(checkInterval))
                    return;
                var poolArray = pools.ToArray();
                var totals = new Dictionary<PoolKey, int>();
                foreach(var pool in poolArray)
                {
                    var unusedItemCount = pool.Value.RemoveIdleItems(itemIdleTimeout);
                    if(unusedItemCount > 0)
                        totals.Add(pool.Key, unusedItemCount);
                }
                if(totals.Count > 0)
                    logger.InfoFormat("UnusedItemsCollecting: \n{0}", string.Join(Environment.NewLine, totals.Select(x => string.Format("  {0}: {1}", x.Key, x.Value))));
                logger.InfoFormat("PoolInfo: \n{0}", string.Join(Environment.NewLine, poolArray.Select(x => string.Format("  {0}: Free: {1}, Busy: {2}", x.Key, x.Value.FreeItemCount, x.Value.BusyItemCount))));
            }
        }

        private int GetPoolItemCountByKey(TItemKey itemKey)
        {
            var totalReplicaCount = replicaHealths.Select(x => x.ReplicaKey).Select(x => GetPool(itemKey, x)).Sum(x => x.TotalCount);
            return totalReplicaCount;
        }

        private Pool<TItem> GetPool(TItemKey itemKey, TReplicaKey replicaKey, bool createNewIfNotExists = true)
        {
            return GetPool(new PoolKey(itemKey, replicaKey), createNewIfNotExists);
        }

        private Pool<TItem> GetPool(PoolKey key, bool createNewIfNotExists = true)
        {
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
            int replicaIndex;
            if(replicaIndicies.TryGetValue(replicaKey, out replicaIndex))
            {
                var replicaHealth = replicaHealths[replicaIndex];
                var healthValue = replicaHealth.Value * aliveRate;
                if(healthValue > aliveHealth) healthValue = aliveHealth;
                replicaHealth.Value = healthValue;
            }
        }

        private readonly Dictionary<TReplicaKey, int> replicaIndicies;
        private readonly ReplicaHealth<TReplicaKey>[] replicaHealths;
        private readonly Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory;
        private readonly Func<TItem, TReplicaKey> getReplicaKeyByItem;
        private readonly Func<TItem, TItemKey> getItemKeyByItem;
        private readonly ConcurrentDictionary<PoolKey, Pool<TItem>> pools;
        private readonly ILog logger = LogManager.GetLogger(typeof(ReplicaSetPool<,,>));
        private readonly Thread unusedItemsCollectorThread;
        private readonly ManualResetEvent disposeEvent;

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
        private const double deadHealth = 0.001;
        private const double aliveRate = 1.5;
        private const double dieRate = 0.7;
    }

    internal static class ReplicaSetPool
    {
        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(TReplicaKey[] replicas, Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory)
            where TItem : class, IDisposable, IPoolKeyContainer<TItemKey, TReplicaKey>, ILiveness
            where TItemKey : IEquatable<TItemKey>
            where TReplicaKey : IEquatable<TReplicaKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, i => i.ReplicaKey, i => i.PoolKey);
        }

        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(
            TReplicaKey[] replicas,
            Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
            Func<TItem, TReplicaKey> getReplicaKeyByItem,
            Func<TItem, TItemKey> getItemKeyByItem,
            TimeSpan? unusedItemsIdleTimeout)
            where TItem : class, IDisposable, ILiveness
            where TItemKey : IEquatable<TItemKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, getReplicaKeyByItem, getItemKeyByItem, unusedItemsIdleTimeout);
        }

        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(TReplicaKey[] replicas, Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory, TimeSpan unusedItemsIdleTimeout)
            where TItem : class, IDisposable, IPoolKeyContainer<TItemKey, TReplicaKey>, ILiveness
            where TItemKey : IEquatable<TItemKey>
            where TReplicaKey : IEquatable<TReplicaKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, i => i.ReplicaKey, i => i.PoolKey, unusedItemsIdleTimeout);
        }
    }
}