using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Core.Pools;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using Vostok.Logging.Abstractions;

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
                              PoolSettings poolSettings,
                              ILog logger,
                              TimeSpan? itemIdleTimeout = null)
        {
            this.logger = logger.ForContext("ReplicaSetPool");
            if (replicas.Count == 0)
                throw new EmptyPoolException("Cannot create empty ReplicaSetPool");
            this.logger.Info("ReplicaSetPool created with client topology: {0}", string.Join(", ", replicas));
            replicaIndicies = new Dictionary<TReplicaKey, int>(replicaKeyComparer);
            replicaHealths = new ReplicaHealth<TReplicaKey>[replicas.Count];
            for (var idx = 0; idx < replicas.Count; idx++)
            {
                var replica = replicas[idx];
                replicaIndicies.Add(replica, idx);
                replicaHealths[idx] = new ReplicaHealth<TReplicaKey>(replica, aliveHealth);
            }
            this.poolFactory = poolFactory;
            this.replicaKeyComparer = replicaKeyComparer;
            this.getReplicaKeyByItem = getReplicaKeyByItem;
            this.getItemKeyByItem = getItemKeyByItem;
            this.poolSettings = poolSettings;

            pools = new ConcurrentDictionary<PoolKey, Pool<TItem>>(new PoolKeyEqualityComparer(replicaKeyComparer, itemKeyComparer));
            disposeEvent = new ManualResetEvent(false);
            checkDeadItemsThread = new Thread(CheckDeadItemsThread)
                {
                    Name = string.Format("CheckDeadConnectionsForThread {0}", string.Join(", ", replicas)),
                    IsBackground = true
                };
            checkDeadItemsThread.Start();

            if (itemIdleTimeout != null)
            {
                this.logger.Info("Item idle timeout: {0}", itemIdleTimeout.Value);
                unusedItemsCollectorThread = new Thread(() => UnusedItemsCollectorProcedure(itemIdleTimeout.Value))
                    {
                        IsBackground = true
                    };
                unusedItemsCollectorThread.Start();
            }
        }

        private void CheckDeadItemsThread()
        {
            var deadReplicaPingInfos = new Dictionary<TReplicaKey, DeadReplicaInfo>(replicaKeyComparer);
            while (true)
            {
                try
                {
                    var deadReplicaKeys = GetDeadReplicaKeys();
                    foreach (var deadReplicaKey in deadReplicaKeys)
                    {
                        if (NeedPingDeadReplica(deadReplicaKey, deadReplicaPingInfos))
                        {
                            var deadReplicaPools = GetAllPoolsWithReplicaKey(deadReplicaKey);
                            foreach (var deadReplicaPool in deadReplicaPools)
                            {
                                try
                                {
                                    TItem connection;
                                    if (TryAcquireNew(deadReplicaPool.Value, out connection))
                                    {
                                        deadReplicaPingInfos.Remove(deadReplicaKey);
                                        GoodReplica(deadReplicaKey);
                                        deadReplicaPool.Value.Release(connection);
                                    }
                                    else
                                    {
                                        UnsuccessfulReplicaPing(deadReplicaPingInfos, deadReplicaKey);
                                    }
                                }
                                catch (Exception exception)
                                {
                                    UnsuccessfulReplicaPing(deadReplicaPingInfos, deadReplicaKey);
                                    logger.Warn(exception, "Error while ping dead replica: Cannot acquire new connection for replica [{0}]", deadReplicaKey);
                                }
                                if (disposeEvent.WaitOne(0))
                                    return;
                            }
                        }
                    }
                    if (disposeEvent.WaitOne(poolSettings.CheckIntervalIncreaseBasis))
                        return;
                }
                catch (Exception exception)
                {
                    logger.Error(exception, "Unexpected error while ping dead replicas.");
                    if (exception is ThreadAbortException)
                        throw; // workaround for https://github.com/dotnet/coreclr/issues/16122 on net471
                }
            }
        }

        private void UnsuccessfulReplicaPing(Dictionary<TReplicaKey, DeadReplicaInfo> deadReplicaPingInfos, TReplicaKey deadReplicaKey)
        {
            deadReplicaPingInfos[deadReplicaKey].Attempts++;
            deadReplicaPingInfos[deadReplicaKey].LastPingDateTime = DateTime.UtcNow;
            BadReplica(deadReplicaKey);
        }

        private TReplicaKey[] GetDeadReplicaKeys()
        {
            return replicaHealths.Where(x => !IsAliveReplica(x.ReplicaKey, x.Value)).Select(x => x.ReplicaKey).ToArray();
        }

        private bool NeedPingDeadReplica(TReplicaKey deadReplica, Dictionary<TReplicaKey, DeadReplicaInfo> replicaPingInfos)
        {
            if (!replicaPingInfos.ContainsKey(deadReplica))
            {
                replicaPingInfos[deadReplica] = new DeadReplicaInfo
                    {
                        Attempts = 0,
                        LastPingDateTime = DateTime.MinValue
                    };
                return true;
            }
            var deadReplicaInfo = replicaPingInfos[deadReplica];
            var nextInterval = TimeSpan.FromMilliseconds(Math.Min(
                poolSettings.MaxCheckInterval.TotalMilliseconds,
                (long)Math.Round(poolSettings.CheckIntervalIncreaseBasis.TotalMilliseconds * Math.Pow(2, Math.Min(20, deadReplicaInfo.Attempts)))));
            return (DateTime.UtcNow - deadReplicaInfo.LastPingDateTime) >= nextInterval;
        }

        private KeyValuePair<PoolKey, Pool<TItem>>[] GetAllPoolsWithReplicaKey(TReplicaKey deadReplica)
        {
            return pools.ToList().Where(x => replicaKeyComparer.Equals(x.Key.ReplicaKey, deadReplica)).ToArray();
        }

        public void Dispose()
        {
            disposeEvent.Set();
            if (!checkDeadItemsThread.Join(TimeSpan.FromSeconds(2)))
                logger.Warn("Cannot await stopping check dead items thread. Skip waiting");
            if (unusedItemsCollectorThread != null)
            {
                if (!unusedItemsCollectorThread.Join(TimeSpan.FromMilliseconds(100)))
                    logger.Warn("UnusedItemsCollector do not completed in 100ms. Skip waiting.");
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
                .Where(x => IsAliveReplica(x.ReplicaKey, x.Value))
                .ShuffleByHealth(x => x.Value, x => new {Health = x.Value, x.ReplicaKey})
                .Any(poolInfo =>
                     TryAcquireExistsOrNew(
                         new PoolKey(itemKey, poolInfo.ReplicaKey),
                         GetDesiredActiveItemCountForReplica(poolInfo.Health, totalReplicaHealth, totalReplicaCount),
                         out result));

            if (!existingAcquired)
            {
                var replicaKeys = replicaHealths.ShuffleByHealth(x => x.Value, x => x.ReplicaKey);

                var exceptions = new List<Exception>();
                foreach (var replicaKey in replicaKeys)
                {
                    var replicaPool = GetPool(itemKey, replicaKey);
                    try
                    {
                        TItem item;
                        if (TryAcquireNew(replicaPool, out item))
                            return item;
                    }
                    catch (Exception e)
                    {
                        logger.Warn(e, "Cannot acquire new connection for replica [{0}]", replicaKey);
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

        private bool IsAliveReplica(TReplicaKey replicaKey, double health)
        {
            return health > (poolSettings.DeadHealth + 0.00001);
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

        internal void BadReplica(TReplicaKey replicaKey)
        {
            int replicaIndex;
            if (replicaIndicies.TryGetValue(replicaKey, out replicaIndex))
            {
                var replicaHealth = replicaHealths[replicaIndex];
                var healthValue = replicaHealth.Value * dieRate;
                if (healthValue < deadHealth) healthValue = deadHealth;
                replicaHealth.Value = healthValue;
            }
        }

        private bool TryAcquireNew(Pool<TItem> replicaPool, out TItem result)
        {
            result = replicaPool.AcquireNew();
            if (!result.IsAlive)
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
            if (pool.TryAcquireExists(out result))
                return true;

            if (pool.TotalCount <= replicaDesiredItemCount)
            {
                try
                {
                    return TryAcquireNew(pool, out result);
                }
                catch (Exception e)
                {
                    logger.Warn(e, "Cannot acquire new connection for replica [{0}]", poolKey.ReplicaKey);
                    BadReplica(poolKey.ReplicaKey);
                    return false;
                }
            }
            return false;
        }

        private void UnusedItemsCollectorProcedure(TimeSpan itemIdleTimeout)
        {
            var checkInterval = TimeSpan.FromMilliseconds(itemIdleTimeout.TotalMilliseconds / 2);
            while (true)
            {
                if (disposeEvent.WaitOne(checkInterval))
                    return;
                var poolArray = pools.ToArray();
                var totals = new Dictionary<PoolKey, int>();
                foreach (var pool in poolArray)
                {
                    var unusedItemCount = pool.Value.RemoveIdleItems(itemIdleTimeout);
                    if (unusedItemCount > 0)
                        totals.Add(pool.Key, unusedItemCount);
                }
                if (totals.Count > 0)
                    logger.Info("UnusedItemsCollecting: \n{0}", string.Join(Environment.NewLine, totals.Select(x => string.Format("  {0}: {1}", x.Key, x.Value))));
                logger.Info("PoolInfo: \n{0}", string.Join(Environment.NewLine, poolArray.Select(x => string.Format("  {0}: Free: {1}, Busy: {2}", x.Key, x.Value.FreeItemCount, x.Value.BusyItemCount))));
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
            if (!createNewIfNotExists)
            {
                Pool<TItem> result;
                if (!pools.TryGetValue(key, out result))
                    throw new InvalidPoolKeyException(string.Format("Pool with key [{0}] does not exists", key));
                return result;
            }
            return pools.GetOrAdd(key, x => poolFactory(x.ItemKey, x.ReplicaKey));
        }

        private void GoodReplica(TReplicaKey replicaKey)
        {
            int replicaIndex;
            if (replicaIndicies.TryGetValue(replicaKey, out replicaIndex))
            {
                var replicaHealth = replicaHealths[replicaIndex];
                var healthValue = replicaHealth.Value * aliveRate;
                if (healthValue > aliveHealth) healthValue = aliveHealth;
                replicaHealth.Value = healthValue;
            }
        }

        private const double aliveHealth = 1.0;
        private const double deadHealth = 0.01;
        private const double aliveRate = 1.5;
        private const double dieRate = 0.7;

        private readonly Dictionary<TReplicaKey, int> replicaIndicies;
        private readonly ReplicaHealth<TReplicaKey>[] replicaHealths;
        private readonly Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory;
        private readonly IEqualityComparer<TReplicaKey> replicaKeyComparer;
        private readonly Func<TItem, TReplicaKey> getReplicaKeyByItem;
        private readonly Func<TItem, TItemKey> getItemKeyByItem;
        private readonly PoolSettings poolSettings;
        private readonly ILog logger;
        private readonly ConcurrentDictionary<PoolKey, Pool<TItem>> pools;
        private readonly Thread unusedItemsCollectorThread;
        private readonly ManualResetEvent disposeEvent;
        private readonly Thread checkDeadItemsThread;

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

        private class DeadReplicaInfo
        {
            public int Attempts { get; set; }
            public DateTime LastPingDateTime { get; set; }
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
                if (ReferenceEquals(x, y)) return true;
                if (ReferenceEquals(null, x) || ReferenceEquals(null, y)) return false;
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
    }

    internal static class ReplicaSetPool
    {
        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(
            TReplicaKey[] replicas,
            Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
            PoolSettings poolSettings,
            ILog logger
            )
            where TItem : class, IDisposable, IPoolKeyContainer<TItemKey, TReplicaKey>, ILiveness
            where TItemKey : IEquatable<TItemKey>
            where TReplicaKey : IEquatable<TReplicaKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, i => i.ReplicaKey, i => i.PoolKey, poolSettings, logger, null);
        }

        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(
            TReplicaKey[] replicas,
            Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
            Func<TItem, TReplicaKey> getReplicaKeyByItem,
            Func<TItem, TItemKey> getItemKeyByItem,
            TimeSpan? unusedItemsIdleTimeout,
            PoolSettings poolSettings,
            ILog logger
            )
            where TItem : class, IDisposable, ILiveness
            where TItemKey : IEquatable<TItemKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, getReplicaKeyByItem, getItemKeyByItem, poolSettings, logger, unusedItemsIdleTimeout);
        }

        public static ReplicaSetPool<TItem, TItemKey, TReplicaKey> Create<TItem, TItemKey, TReplicaKey>(
            TReplicaKey[] replicas,
            Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory,
            PoolSettings poolSettings,
            TimeSpan unusedItemsIdleTimeout,
            ILog logger
            )
            where TItem : class, IDisposable, IPoolKeyContainer<TItemKey, TReplicaKey>, ILiveness
            where TItemKey : IEquatable<TItemKey>
            where TReplicaKey : IEquatable<TReplicaKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, i => i.ReplicaKey, i => i.PoolKey, poolSettings, logger, unusedItemsIdleTimeout);
        }
    }
}