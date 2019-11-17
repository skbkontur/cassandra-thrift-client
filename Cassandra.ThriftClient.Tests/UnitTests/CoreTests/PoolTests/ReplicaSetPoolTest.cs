using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Core.GenericPool;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions;

using Vostok.Logging.Abstractions;

namespace Cassandra.ThriftClient.Tests.UnitTests.CoreTests.PoolTests
{
    [TestFixture]
    public class ReplicaSetPoolTest
    {
        [Test]
        public void TestAcquireWithoutRegisteredKeys()
        {
            Assert.Throws<EmptyPoolException>(() => CreateReplicaSetPool(0));
        }

        [Test]
        public void TestAcquireWithItemKeyIsNull()
        {
            using (var pool = CreateReplicaSetPool())
            {
                var item1 = pool.Acquire(null);
                pool.Release(item1);
                var item2 = pool.Acquire(null);
                Assert.That(item2, Is.EqualTo(item1));
            }
        }

        [Test]
        public void TestDisposeItemsAfterPoolDispose()
        {
            Item item1;
            Item item2;
            Item item3;
            using (var pool = CreateReplicaSetPool())
            {
                item1 = pool.Acquire(new ItemKey("key1"));
                item2 = pool.Acquire(new ItemKey("key1"));
                item3 = pool.Acquire(new ItemKey("key1"));
            }
            Assert.That(item1.Disposed);
            Assert.That(item2.Disposed);
            Assert.That(item3.Disposed);
        }

        [Test]
        public void TestAcquireItemWithSameKey()
        {
            using (var pool = CreateReplicaSetPool())
            {
                var item1 = pool.Acquire(new ItemKey("key1"));
                pool.Release(item1);
                var item2 = pool.Acquire(new ItemKey("key1"));
                Assert.That(item2, Is.EqualTo(item1));
            }
        }

        [Test]
        public void TestAcquireItemWithDifferentKeys()
        {
            using (var pool = CreateReplicaSetPool())
            {
                var item1 = pool.Acquire(new ItemKey("key1"));
                pool.Release(item1);
                var item2 = pool.Acquire(new ItemKey("key2"));
                Assert.That(item2, Is.Not.EqualTo(item1));
            }
        }

        [Test]
        public void TestReleaseItemOfNotRegisterPool()
        {
            using (var pool = CreateReplicaSetPool())
            {
                var item1 = pool.Acquire(new ItemKey("key2"));
                pool.Release(item1);
                var item2 = new Item(new ItemKey("key2"), new ReplicaKey("replica2"));
                Assert.Throws<InvalidPoolKeyException>(() => pool.Release(item2));
            }
        }

        [Test]
        public void TestReleaseItemNotAcquiredFromPool()
        {
            using (var pool = CreateReplicaSetPool())
            {
                var item1 = pool.Acquire(new ItemKey("key1"));
                pool.Release(item1);
                var item2 = new Item(new ItemKey("key1"), new ReplicaKey("replica1"));
                Assert.Throws<FailedReleaseItemException>(() => pool.Release(item2));
            }
        }

        [Test]
        public void TestAcquireItemFromKeysWithHealthNodes()
        {
            using (var pool = CreateReplicaSetPool(2))
            {
                var itemKey = new ItemKey("key1");

                var acquiredItems = Enumerable
                    .Range(0, 100)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            return new[] {item1, item2};
                        })
                    .ToList();

                var acquiredItemCount = acquiredItems
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(acquiredItemCount[new ReplicaKey("replica1")], Is.InRange(80, 120));
                Assert.That(acquiredItemCount[new ReplicaKey("replica2")], Is.InRange(80, 120));

                acquiredItems.ForEach(pool.Release);

                var reacquiredItems = Enumerable
                    .Range(0, 10000)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            pool.Release(item1);
                            pool.Release(item2);
                            return new[] {item1, item2};
                        })
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(reacquiredItems[new ReplicaKey("replica1")], Is.InRange(9000, 11000));
                Assert.That(reacquiredItems[new ReplicaKey("replica2")], Is.InRange(9000, 11000));
            }
        }

        [Test]
        public void TestAcquireItemFromKeysWithPartiallyBadNodes()
        {
            using (var pool = CreateReplicaSetPool(2))
            {
                var itemKey = new ItemKey("key1");
                pool.BadReplica(new ReplicaKey("replica2"));
                pool.BadReplica(new ReplicaKey("replica2"));
                pool.BadReplica(new ReplicaKey("replica2"));
                pool.BadReplica(new ReplicaKey("replica2"));
                pool.BadReplica(new ReplicaKey("replica2")); // Health: 0.168

                var acquiredItems = Enumerable
                    .Range(0, 100)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            return new[] {item1, item2};
                        })
                    .ToList();

                var acquiredItemCount = acquiredItems
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(acquiredItemCount[new ReplicaKey("replica1")], Is.InRange(160, 180));
                Assert.That(acquiredItemCount[new ReplicaKey("replica2")], Is.InRange(20, 40));

                acquiredItems.ForEach(pool.Release);

                var reacquiredItems = Enumerable
                    .Range(0, 10000)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            pool.Release(item1);
                            pool.Release(item2);
                            return new[] {item1, item2};
                        })
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(reacquiredItems[new ReplicaKey("replica1")], Is.InRange(16500, 17500));
                Assert.That(reacquiredItems[new ReplicaKey("replica2")], Is.InRange(2500, 3500));
            }
        }

        [Test]
        public void TestAcquireOnlyLiveItemsWithDeadNode()
        {
            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(
                new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")},
                (x, r) => r.Name == "replica2" ?
                              new Pool<Item>(y => new Item(x, r) {IsAlive = false}, new SilentLog()) :
                              new Pool<Item>(y => new Item(x, r), new SilentLog())))
            {
                var itemKey = new ItemKey("key1");

                var items = Enumerable
                    .Range(0, 1)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            Assert.That(item1.IsAlive);
                            Assert.That(item2.IsAlive);
                            return new[] {item1, item2};
                        })
                    .ToList();

                items.ForEach(pool.Release);
            }
        }

        [Test]
        public void TestCreationItemsOnDeadReplica()
        {
            const int attemptCount = 100;
            const int itemCount = 100;
            var deadNodeAttemptCount = 0;

            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(
                new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")},
                (x, r) => r.Name == "replica2" ?
                              new Pool<Item>(y =>
                                  {
                                      deadNodeAttemptCount++;
                                      return new Item(x, r) {IsAlive = false};
                                  }, new SilentLog()) :
                              new Pool<Item>(y => new Item(x, r), new SilentLog())))
            {
                var itemKey = new ItemKey("key1");

                Enumerable
                    .Range(0, attemptCount)
                    .ToList()
                    .ForEach(n =>
                        {
                            var items = Enumerable.Range(0, itemCount).Select(x => pool.Acquire(itemKey)).ToList();
                            items.ForEach(pool.Good);
                            items.ForEach(pool.Release);
                        });

                Assert.That(deadNodeAttemptCount, Is.InRange(0, 0.02 * attemptCount * itemCount));
            }
        }

        [Test]
        public void TestCreationItemsAfterDeadReplica()
        {
            const int attemptCount = 100;
            const int itemCount = 100;
            var deadNodeAttemptCount = 0;

            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(
                new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")},
                (x, r) => r.Name == "replica2" ?
                              new Pool<Item>(y =>
                                  {
                                      deadNodeAttemptCount++;
                                      return new Item(x, r) {IsAlive = false};
                                  }, new SilentLog()) :
                              new Pool<Item>(y => new Item(x, r), new SilentLog())))
            {
                Enumerable.Range(0, 100).ToList().ForEach(x => pool.BadReplica(new ReplicaKey("replica2"))); // Health: 0.01

                var itemKey = new ItemKey("key1");

                Enumerable
                    .Range(0, attemptCount)
                    .ToList()
                    .ForEach(n =>
                        {
                            var items = Enumerable.Range(0, itemCount).Select(x => pool.Acquire(itemKey)).ToList();
                            items.ForEach(pool.Good);
                            items.ForEach(pool.Release);
                        });

                Assert.That(deadNodeAttemptCount, Is.InRange(0, 0.015 * attemptCount * itemCount));
            }
        }

        [Test]
        public void TestAcquireNewFromDeadNode()
        {
            var acquireFromDeadNodeCount = 0;
            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")}, (x, r) =>
                {
                    if (r.Name == "replica2")
                    {
                        return new Pool<Item>(y =>
                            {
                                acquireFromDeadNodeCount++;
                                return new Item(x, r) {IsAlive = false};
                            }, new SilentLog());
                    }
                    return new Pool<Item>(y => new Item(x, r), new SilentLog());
                }))
            {
                var itemKey = new ItemKey("key1");

                var acquiredItems = Enumerable
                    .Range(0, 100)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            Assert.That(item1.IsAlive);
                            Assert.That(item2.IsAlive);
                            return new[] {item1, item2};
                        })
                    .ToList();

                Assert.That(acquireFromDeadNodeCount, Is.InRange(0, 20));

                acquiredItems.ForEach(pool.Release);

                var reacquiredItems = Enumerable
                    .Range(0, 2000)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire(itemKey);
                            var item2 = pool.Acquire(itemKey);
                            pool.Good(item1);
                            pool.Good(item2);
                            pool.Release(item1);
                            pool.Release(item2);
                            return new[] {item1, item2};
                        })
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(acquireFromDeadNodeCount, Is.InRange(0, 20));
                Assert.That(!reacquiredItems.ContainsKey(new ReplicaKey("replica2")));
            }
        }

        [Test]
        public void TestAcquireNewWithDeadNodes()
        {
            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")}, (x, z) => new Pool<Item>(y => new Item(x, z) {IsAlive = false}, new SilentLog())))
            {
                Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire(new ItemKey("1")));
                Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire(new ItemKey("1")));
            }
        }

        [Test]
        public void TestAcquireConnectionWithExceptionInOnePool()
        {
            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")}, (x, z) => new Pool<Item>(y =>
                {
                    if (z.Name == "replica1")
                        throw new Exception("FakeException");
                    return new Item(x, z);
                }, new SilentLog())))
            {
                for (var i = 0; i < 1000; i++)
                {
                    var item = pool.Acquire(new ItemKey("1"));
                    Assert.That(item.ReplicaKey.Name, Is.EqualTo("replica2"));
                }
            }
        }

        [Test]
        public void TestTryAcquireConnectionWithExceptionAllPools()
        {
            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(new[] {new ReplicaKey("replica1"), new ReplicaKey("replica2")}, (x, z) => new Pool<Item>(y => { throw new Exception("FakeException"); }, new SilentLog())))
            {
                for (var i = 0; i < 1000; i++)
                {
                    try
                    {
                        pool.Acquire(new ItemKey("1"));
                        Assert.Fail();
                    }
                    catch (Exception exception)
                    {
                        Assert.That(exception is AllItemsIsDeadExceptions);
                        Assert.That(exception.InnerException, Is.Not.Null);
                        Assert.That(exception.InnerException.Message, Is.EqualTo("FakeException"));
                        Assert.That((exception as AggregateException).InnerExceptions.Count, Is.EqualTo(2));
                        Assert.That((exception as AggregateException).InnerExceptions[0].Message, Is.EqualTo("FakeException"));
                    }
                }
            }
        }

        [Test]
        public void TestRemoveUnusedConnection()
        {
            using (var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>(new[] {new ReplicaKey("replica1")}, (x, z) => new Pool<Item>(y => new Item(x, z), new SilentLog()), PoolSettings.CreateDefault(), TimeSpan.FromMilliseconds(100), new SilentLog()))
            {
                var item1 = pool.Acquire(null);
                var item2 = pool.Acquire(null);
                pool.Release(item1);

                Thread.Sleep(500);

                pool.Release(item2);

                var item3 = pool.Acquire(null);
                var item4 = pool.Acquire(null);

                Assert.That(item3, Is.EqualTo(item2));
                Assert.That(item4, Is.Not.EqualTo(item1) & Is.Not.EqualTo(item2));
            }
        }

        [Test]
        public void TestRemoveAcquiredConnectionFromPool()
        {
            using (var pool = CreatePool<Item, ItemKey, ReplicaKey>(new[] {new ReplicaKey("replica1")}, (x, z) => new Pool<Item>(y => new Item(x, z), new SilentLog())))
            {
                var item1 = pool.Acquire(null);
                var item2 = pool.Acquire(null);
                pool.Release(item1);
                pool.Remove(item2);

                var item3 = pool.Acquire(null);
                var item4 = pool.Acquire(null);

                Assert.That(item3, Is.EqualTo(item1));
                Assert.That(item4, Is.Not.EqualTo(item1) & Is.Not.EqualTo(item2));
            }
        }

        [Test]
        public void TestOneDeadReplicaDoCreateConnectionsWhenAcquire()
        {
            using (var poolManager = new ReplicaSetPoolManager(2, new PoolSettings {DeadHealth = 0.01, MaxCheckInterval = TimeSpan.FromSeconds(1), CheckIntervalIncreaseBasis = TimeSpan.FromMilliseconds(100)}))
            {
                SimulateLoad(poolManager, threads : 10, operations : 1000);
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(5, 10));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(5, 10));

                poolManager.MakeDeadReplica(0);
                SimulateLoad(poolManager, threads : 10, operations : 1000);

                poolManager.ResetCounters();
                SimulateLoad(poolManager, threads : 10, operations : 1000);
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(0, 1));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(0, 10));
                Assert.That(poolManager.GetAcquiredCount(0), Is.EqualTo(0));
                Assert.That(poolManager.GetAcquiredCount(1), Is.EqualTo(1000 * 10));
            }
        }

        [Test]
        public void TestOneDeadReplicaTryCreateConnections()
        {
            using (var poolManager = new ReplicaSetPoolManager(2, new PoolSettings {DeadHealth = 0.01, MaxCheckInterval = TimeSpan.FromSeconds(1), CheckIntervalIncreaseBasis = TimeSpan.FromMilliseconds(100)}))
            {
                SimulateLoad(poolManager, threads : 10, operations : 1000);
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(5, 10));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(5, 10));

                poolManager.MakeDeadReplica(0);
                SimulateLoad(poolManager, threads : 10, operations : 1000);

                poolManager.ResetCounters();
                SimulateLoad(poolManager, threads : 10, operations : 1000, interval : TimeSpan.FromSeconds(10));
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(10, 20));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(0, 10));
                Assert.That(poolManager.GetAcquiredCount(0), Is.EqualTo(0));
                Assert.That(poolManager.GetAcquiredCount(1), Is.EqualTo(1000 * 10));
            }
        }

        [Test]
        public void TestOneReplicaAliveAfterDead()
        {
            using (var poolManager = new ReplicaSetPoolManager(2, new PoolSettings {DeadHealth = 0.01, MaxCheckInterval = TimeSpan.FromSeconds(1), CheckIntervalIncreaseBasis = TimeSpan.FromMilliseconds(100)}))
            {
                SimulateLoad(poolManager, threads : 10, operations : 1000);
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(5, 10));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(5, 10));

                poolManager.MakeDeadReplica(0);
                SimulateLoad(poolManager, threads : 10, operations : 1000, interval : TimeSpan.FromSeconds(10));

                poolManager.MakeLiveReplica(0);
                poolManager.ResetCounters();
                SimulateLoad(poolManager, threads : 10, operations : 1000, interval : TimeSpan.FromSeconds(20));
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(7, 17));
                Assert.That(poolManager.GetAcquiredCount(0), Is.InRange(1000 * 3, 1000 * 7));
                Assert.That(poolManager.GetAcquiredCount(1), Is.InRange(1000 * 3, 1000 * 7));
            }
        }

        [Test]
        public void TestAllReplicaDeadCauseSynchronousConnect()
        {
            using (var poolManager = new ReplicaSetPoolManager(2, new PoolSettings {DeadHealth = 0.01, MaxCheckInterval = TimeSpan.FromSeconds(1), CheckIntervalIncreaseBasis = TimeSpan.FromMilliseconds(100)}))
            {
                SimulateLoad(poolManager, threads : 10, operations : 1000);

                poolManager.MakeDeadReplica(0);
                poolManager.MakeDeadReplica(1);
                poolManager.ResetCounters();
                Assert.Throws<AllItemsIsDeadExceptions>(() => poolManager.Acquire());
                Assert.Throws<AllItemsIsDeadExceptions>(() => poolManager.Acquire());
                Assert.Throws<AllItemsIsDeadExceptions>(() => poolManager.Acquire());
                Assert.Throws<AllItemsIsDeadExceptions>(() => poolManager.Acquire());

                Assert.That(poolManager.GetCreateCount(0), Is.InRange(4, 4 * 2));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(4, 4 * 2));
            }
        }

        [Test]
        public void TestAliveAfterAllReplicaDead()
        {
            using (var poolManager = new ReplicaSetPoolManager(2, new PoolSettings {DeadHealth = 0.01, MaxCheckInterval = TimeSpan.FromSeconds(1), CheckIntervalIncreaseBasis = TimeSpan.FromMilliseconds(100)}))
            {
                SimulateLoad(poolManager, threads : 10, operations : 1000);

                poolManager.MakeDeadReplica(0);
                poolManager.MakeDeadReplica(1);
                poolManager.ResetCounters();
                for (var i = 0; i < 1000; i++)
                    Assert.Throws<AllItemsIsDeadExceptions>(() => poolManager.Acquire());

                poolManager.MakeLiveReplica(0);
                poolManager.MakeLiveReplica(1);
                poolManager.ResetCounters();
                SimulateLoad(poolManager, threads : 10, operations : 1000);
                Assert.That(poolManager.GetCreateCount(0), Is.InRange(7, 17));
                Assert.That(poolManager.GetCreateCount(1), Is.InRange(7, 17));
                Assert.That(poolManager.GetAcquiredCount(0), Is.InRange(1000 * 3, 1000 * 7));
                Assert.That(poolManager.GetAcquiredCount(1), Is.InRange(1000 * 3, 1000 * 7));
            }
        }

        private void SimulateLoad(ReplicaSetPoolManager poolManager, int threads, int operations, TimeSpan? interval = null)
        {
            var operationsInterval = interval == null ? (TimeSpan?)null : TimeSpan.FromTicks(interval.Value.Ticks / operations);
            for (var i = 0; i < operations; i++)
            {
                var acquiredItems = Enumerable.Range(0, threads).Select(x => poolManager.Acquire()).ToArray();
                if (operationsInterval != null)
                    Thread.Sleep(operationsInterval.Value);
                foreach (var acquiredItem in acquiredItems)
                {
                    poolManager.Good(acquiredItem);
                    poolManager.Release(acquiredItem);
                }
            }
        }

        private static ReplicaSetPool<Item, ItemKey, ReplicaKey> CreateReplicaSetPool(int replicaCount = 1, string nameFormat = "replica{0}")
        {
            var replicas = Enumerable
                .Range(1, replicaCount)
                .Select(n => string.Format(nameFormat, n))
                .Select(x => new ReplicaKey(x))
                .ToArray();
            var pool = CreatePool<Item, ItemKey, ReplicaKey>(replicas, (x, z) => new Pool<Item>(y => new Item(x, z), new SilentLog()));
            return pool;
        }

        private static ReplicaSetPool<TItem, TItemKey, TReplicaKey> CreatePool<TItem, TItemKey, TReplicaKey>(
            TReplicaKey[] replicas,
            Func<TItemKey, TReplicaKey, Pool<TItem>> poolFactory
            )
            where TItem : class, IDisposable, IPoolKeyContainer<TItemKey, TReplicaKey>, ILiveness
            where TItemKey : IEquatable<TItemKey>
            where TReplicaKey : IEquatable<TReplicaKey>
        {
            return new ReplicaSetPool<TItem, TItemKey, TReplicaKey>(replicas, poolFactory, EqualityComparer<TReplicaKey>.Default, EqualityComparer<TItemKey>.Default, i => i.ReplicaKey, i => i.PoolKey, PoolSettings.CreateDefault(), new SilentLog(), null);
        }

        private class ItemKey : IEquatable<ItemKey>
        {
            public ItemKey(string value)
            {
                Value = value;
            }

            public bool Equals(ItemKey other)
            {
                return string.Equals(Value, other.Value);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((ItemKey)obj);
            }

            public override int GetHashCode()
            {
                return (Value != null ? Value.GetHashCode() : 0);
            }

            private string Value { get; }
        }

        private class ReplicaKey : IEquatable<ReplicaKey>
        {
            public ReplicaKey(string name)
            {
                Name = name;
            }

            public bool Equals(ReplicaKey other)
            {
                if (ReferenceEquals(null, other)) return false;
                if (ReferenceEquals(this, other)) return true;
                return string.Equals(Name, other.Name);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                return obj.GetType() == GetType() && Equals((ReplicaKey)obj);
            }

            public override int GetHashCode()
            {
                return (Name != null ? Name.GetHashCode() : 0);
            }

            public string Name { get; }
        }

        private class Item : IDisposable, IPoolKeyContainer<ItemKey, ReplicaKey>, ILiveness
        {
            public Item(ItemKey key, ReplicaKey replicaKey, Func<ReplicaKey, bool> isAliveFunc = null)
            {
                this.isAliveFunc = isAliveFunc;
                PoolKey = key;
                ReplicaKey = replicaKey;
                isAlive = isAliveFunc == null || isAliveFunc(ReplicaKey);
            }

            public bool Disposed { get; private set; }

            public void Dispose()
            {
                Disposed = true;
            }

            public bool IsAlive
            {
                get
                {
                    if (isAliveFunc != null)
                        return isAliveFunc(ReplicaKey);
                    return isAlive;
                }
                set
                {
                    if (isAliveFunc != null)
                        throw new Exception("Cannot set IsAlive flag if isAliveFunc provided");
                    isAlive = value;
                }
            }

            public ItemKey PoolKey { get; }
            public ReplicaKey ReplicaKey { get; }
            private readonly Func<ReplicaKey, bool> isAliveFunc;

            private bool isAlive;
        }

        private class ReplicaSetPoolManager : IDisposable
        {
            public ReplicaSetPoolManager(int count, PoolSettings poolSettings)
            {
                replicaInfos = Enumerable.Range(0, count).ToDictionary(x => x, x => new ReplicaInfo
                    {
                        Key = new ReplicaKey("replica" + x),
                        IsDead = false
                    });
                replicaSetPool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>(
                    replicaInfos.Values.Select(x => x.Key).ToArray(),
                    (x, z) => new Pool<Item>(y => CreateReplicaConnection(z, x), new SilentLog()),
                    poolSettings, new SilentLog());
            }

            private Item CreateReplicaConnection(ReplicaKey replicaKey, ItemKey itemKey)
            {
                var replicaNumber = Int32.Parse(replicaKey.Name.Replace("replica", ""));
                replicaInfos[replicaNumber].IncrementCreationCount();
                if (replicaInfos[replicaNumber].IsDead)
                    throw new Exception($"Replica {replicaNumber} is dead");
                return new Item(itemKey, replicaKey, i => !replicaInfos[replicaNumber].IsDead);
            }

            public Item Acquire()
            {
                var result = replicaSetPool.Acquire(new ItemKey("key"));
                var replicaNumber = Int32.Parse(result.ReplicaKey.Name.Replace("replica", ""));
                replicaInfos[replicaNumber].IncrementAcquiredCount();
                return result;
            }

            public void Release(Item item)
            {
                replicaSetPool.Release(item);
            }

            public void Dispose()
            {
                replicaSetPool.Dispose();
            }

            public void MakeDeadReplica(int replicaNumber)
            {
                replicaInfos[replicaNumber].IsDead = true;
            }

            public void Good(Item item)
            {
                replicaSetPool.Good(item);
            }

            public object GetCreateCount(int replicaNumber)
            {
                return replicaInfos[replicaNumber].CreationCount;
            }

            public object GetAcquiredCount(int replicaNumber)
            {
                return replicaInfos[replicaNumber].AcquiredCount;
            }

            public void ResetCounters()
            {
                foreach (var value in replicaInfos.Values)
                    value.ResetCreationCount();
            }

            public void MakeLiveReplica(int replicaNumber)
            {
                replicaInfos[replicaNumber].IsDead = false;
            }

            private readonly ReplicaSetPool<Item, ItemKey, ReplicaKey> replicaSetPool;
            private readonly Dictionary<int, ReplicaInfo> replicaInfos;

            private class ReplicaInfo
            {
                public ReplicaKey Key { get; set; }
                public bool IsDead { get; set; }
                public int CreationCount => creationCount;
                public int AcquiredCount => acquiredCount;

                public void IncrementCreationCount()
                {
                    Interlocked.Increment(ref creationCount);
                }

                public void ResetCreationCount()
                {
                    Interlocked.Exchange(ref creationCount, 0);
                    Interlocked.Exchange(ref acquiredCount, 0);
                }

                public void IncrementAcquiredCount()
                {
                    Interlocked.Increment(ref acquiredCount);
                }

                private int creationCount;
                private int acquiredCount;
            }
        }
    }
}