using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;

namespace Cassandra.Tests.CoreTests.PoolTests
{
    [TestFixture]
    public class ReplicaSetPoolTest
    {
        [Test]
        public void TestAcquireWithoutRegisteredKeys()
        {
            var pool = CreateReplicaSetPool(0);
            Assert.Throws<EmptyPoolException>(() => pool.Acquire(new ItemKey("key1")));
        }

        [Test]
        public void TestAcquireWithItemKeyIsNull()
        {
            var pool = CreateReplicaSetPool();
            var item1 = pool.Acquire(null);
            pool.Release(item1);
            var item2 = pool.Acquire(null);
            Assert.That(item2, Is.EqualTo(item1));
        }

        [Test]
        public void TestAcquireItemWithSameKey()
        {
            var pool = CreateReplicaSetPool();
            var item1 = pool.Acquire(new ItemKey("key1"));
            pool.Release(item1);
            var item2 = pool.Acquire(new ItemKey("key1"));
            Assert.That(item2, Is.EqualTo(item1));
        }

        [Test]
        public void TestAcquireItemWithDifferentKeys()
        {
            var pool = CreateReplicaSetPool();
            var item1 = pool.Acquire(new ItemKey("key1"));
            pool.Release(item1);
            var item2 = pool.Acquire(new ItemKey("key2"));
            Assert.That(item2, Is.Not.EqualTo(item1));
        }

        [Test]
        public void TestReleaseItemOfNotRegisterPool()
        {
            var pool = CreateReplicaSetPool();
            var item1 = pool.Acquire(new ItemKey("key2"));
            pool.Release(item1);
            var item2 = new Item(new ItemKey("key2"), new ReplicaKey("replica2"));
            Assert.Throws<InvalidPoolKeyException>(() => pool.Release(item2));
        }

        [Test]
        public void TestReleaseItemNotAcquiredFromPool()
        {
            var pool = CreateReplicaSetPool();
            var item1 = pool.Acquire(new ItemKey("key1"));
            pool.Release(item1);
            var item2 = new Item(new ItemKey("key1"), new ReplicaKey("replica1"));
            Assert.Throws<FailedReleaseItemException>(() => pool.Release(item2));
        }

        [Test]
        public void TestAcquireItemFromKeysWithHealthNodes()
        {
            var pool = CreateReplicaSetPool(2);
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

        [Test]
        public void TestAcquireItemFromKeysWithPartiallyBadNodes()
        {
            var pool = CreateReplicaSetPool(2);
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

        [Test]
        public void TestAliveAfterDead()
        {
            var pool = CreateReplicaSetPool(2);
            var itemKey = new ItemKey("key1");
            Enumerable.Range(0, 100).ToList().ForEach(x => pool.BadReplica(new ReplicaKey("replica2"))); // Health: 0.01

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

            Assert.That(acquiredItemCount[new ReplicaKey("replica1")], Is.InRange(190, 200));
            int count;
            Assert.That(acquiredItemCount.TryGetValue(new ReplicaKey("replica2"), out count) ? count : 0, Is.InRange(0, 10));

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

            Assert.That(reacquiredItems[new ReplicaKey("replica1")], Is.InRange(2000, 2700));
            Assert.That(reacquiredItems[new ReplicaKey("replica2")], Is.InRange(1300, 2000));
        }

        [Test]
        public void TestAcquireOnlyLiveItemsWithDeadNode()
        {
            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>(
                (x, r) => r.Name == "replica2" ?
                              new Pool<Item>(y => new Item(x, r) {IsAlive = false}) :
                              new Pool<Item>(y => new Item(x, r))))
            {
                var itemKey = new ItemKey("key1");
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

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
        public void TestAcquireNewFromDeadNode()
        {
            var acquireFromDeadNodeCount = 0;
            var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, r) =>
                {
                    if(r.Name == "replica2")
                    {
                        return new Pool<Item>(y =>
                            {
                                acquireFromDeadNodeCount++;
                                return new Item(x, r) {IsAlive = false};
                            });
                    }
                    return new Pool<Item>(y => new Item(x, r));
                });
            pool.RegisterReplica(new ReplicaKey("replica1"));
            pool.RegisterReplica(new ReplicaKey("replica2"));
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

        [Test]
        public void TestAcquireNewWithDeadNodes()
        {
            var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) => new Pool<Item>(y => new Item(x, z) { IsAlive = false }));
            pool.RegisterReplica(new ReplicaKey("replica1"));
            pool.RegisterReplica(new ReplicaKey("replica2"));

            Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire(new ItemKey("1")));
            Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire(new ItemKey("1")));
        }

        private static ReplicaSetPool<Item, ItemKey, ReplicaKey> CreateReplicaSetPool(int replicaCount = 1, string nameFormat = "replica{0}")
        {
            var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) => new Pool<Item>(y => new Item(x, z)));
            Enumerable
                .Range(1, replicaCount)
                .Select(n => string.Format(nameFormat, n))
                .Select(x => new ReplicaKey(x))
                .ToList()
                .ForEach(pool.RegisterReplica);
            return (ReplicaSetPool<Item, ItemKey, ReplicaKey>)pool;
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
                if(ReferenceEquals(null, obj)) return false;
                if(ReferenceEquals(this, obj)) return true;
                if(obj.GetType() != GetType()) return false;
                return Equals((ItemKey)obj);
            }

            public override int GetHashCode()
            {
                return (Value != null ? Value.GetHashCode() : 0);
            }

            private string Value { get; set; }
        }

        private class ReplicaKey : IEquatable<ReplicaKey>
        {
            public ReplicaKey(string name)
            {
                Name = name;
            }

            public bool Equals(ReplicaKey other)
            {
                if(ReferenceEquals(null, other)) return false;
                if(ReferenceEquals(this, other)) return true;
                return string.Equals(Name, other.Name);
            }

            public override bool Equals(object obj)
            {
                if(ReferenceEquals(null, obj)) return false;
                if(ReferenceEquals(this, obj)) return true;
                return obj.GetType() == GetType() && Equals((ReplicaKey)obj);
            }

            public override int GetHashCode()
            {
                return (Name != null ? Name.GetHashCode() : 0);
            }

            public string Name { get; private set; }
        }

        private class Item : IDisposable, IPoolKeyContainer<ItemKey, ReplicaKey>, ILiveness
        {
            public Item(ItemKey key, ReplicaKey replicaKey)
            {
                PoolKey = key;
                ReplicaKey = replicaKey;
                IsAlive = true;
            }

            public void Dispose()
            {
            }

            public bool IsAlive { get; set; }
            public ItemKey PoolKey { get; private set; }
            public ReplicaKey ReplicaKey { get; private set; }
        }
    }
}