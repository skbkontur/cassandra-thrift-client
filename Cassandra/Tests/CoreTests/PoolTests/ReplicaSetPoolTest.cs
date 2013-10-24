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
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            Assert.Throws<EmptyPoolException>(() => pool.Acquire());
        }

        [Test]
        public void TestAcquireItem()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            var item1 = pool.Acquire();
            pool.Release(item1);
            var item2 = pool.Acquire();
            Assert.That(item2, Is.EqualTo(item1));
        }

        [Test]
        public void TestReleaseItemOfNotRegisterPool()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            var item1 = pool.Acquire();
            pool.Release(item1);
            var item2 = new Item(new ItemKey("key2"));
            Assert.Throws<InvalidPoolKeyException>(() => pool.Release(item2));
        }

        [Test]
        public void TestReleaseItemNotAcquiredFromPool()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            var item1 = pool.Acquire();
            pool.Release(item1);
            var item2 = new Item(new ItemKey("key1"));
            Assert.Throws<FailedReleaseItemException>(() => pool.Release(item2));
        }

        [Test]
        public void TestAcquireItemFromKeysWithHealthNodes()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            pool.RegisterKey(new ItemKey("key2"));

            var acquiredItems = Enumerable
                .Range(0, 100)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        return new[] {item1, item2};
                    })
                .ToList();

            var acquiredItemCount = acquiredItems
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(acquiredItemCount[new ItemKey("key1")], Is.InRange(80, 120));
            Assert.That(acquiredItemCount[new ItemKey("key2")], Is.InRange(80, 120));

            acquiredItems.ForEach(pool.Release);

            var reacquiredItems = Enumerable
                .Range(0, 10000)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        pool.Release(item1);
                        pool.Release(item2);
                        return new[] {item1, item2};
                    })
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(reacquiredItems[new ItemKey("key1")], Is.InRange(9000, 11000));
            Assert.That(reacquiredItems[new ItemKey("key2")], Is.InRange(9000, 11000));
        }

        [Test]
        public void TestAcquireItemFromKeysWithPartiallyBadNodes()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            pool.RegisterKey(new ItemKey("key2"));
            pool.Bad(new ItemKey("key2"));
            pool.Bad(new ItemKey("key2"));
            pool.Bad(new ItemKey("key2"));
            pool.Bad(new ItemKey("key2"));
            pool.Bad(new ItemKey("key2")); // Health: 0.168

            var acquiredItems = Enumerable
                .Range(0, 100)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        return new[] {item1, item2};
                    })
                .ToList();

            var acquiredItemCount = acquiredItems
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(acquiredItemCount[new ItemKey("key1")], Is.InRange(160, 180));
            Assert.That(acquiredItemCount[new ItemKey("key2")], Is.InRange(20, 40));

            acquiredItems.ForEach(pool.Release);

            var reacquiredItems = Enumerable
                .Range(0, 10000)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        pool.Release(item1);
                        pool.Release(item2);
                        return new[] {item1, item2};
                    })
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(reacquiredItems[new ItemKey("key1")], Is.InRange(16500, 17500));
            Assert.That(reacquiredItems[new ItemKey("key2")], Is.InRange(2500, 3500));
        }

        [Test]
        public void TestAliveAfterDead()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            pool.RegisterKey(new ItemKey("key2"));
            Enumerable.Range(0, 100).ToList().ForEach(x => pool.Bad(new ItemKey("key2"))); // Health: 0.01

            var acquiredItems = Enumerable
                .Range(0, 100)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        return new[] {item1, item2};
                    })
                .ToList();

            var acquiredItemCount = acquiredItems
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(acquiredItemCount[new ItemKey("key1")], Is.InRange(190, 200));
            Assert.That(acquiredItemCount[new ItemKey("key2")], Is.InRange(0, 10) | Throws.InstanceOf<KeyNotFoundException>());

            acquiredItems.ForEach(pool.Release);

            var reacquiredItems = Enumerable
                .Range(0, 2000)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        pool.Good(item1.PoolKey);
                        pool.Good(item2.PoolKey);
                        pool.Release(item1);
                        pool.Release(item2);
                        return new[] {item1, item2};
                    })
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(reacquiredItems[new ItemKey("key1")], Is.InRange(2050, 2350));
            Assert.That(reacquiredItems[new ItemKey("key2")], Is.InRange(1650, 1950));
        }

        [Test]
        public void TestAcquireOnlyLiveItemsWithDeadNode()
        {
            using(var pool = new ReplicaSetPool<Item, ItemKey>(
                x => x.Value == "key2" ?
                         new Pool<Item>(y => new Item(x) {IsAlive = false}) :
                         new Pool<Item>(y => new Item(x))))
            {
                pool.RegisterKey(new ItemKey("key1"));
                pool.RegisterKey(new ItemKey("key2"));

                var items = Enumerable
                    .Range(0, 1)
                    .SelectMany(n =>
                        {
                            var item1 = pool.Acquire();
                            var item2 = pool.Acquire();
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
            var pool = new ReplicaSetPool<Item, ItemKey>(x =>
                {
                    if(x.Value == "key2")
                    {
                        return new Pool<Item>(y =>
                            {
                                acquireFromDeadNodeCount++;
                                return new Item(x) {IsAlive = false};
                            });
                    }
                    return new Pool<Item>(y => new Item(x));
                });
            pool.RegisterKey(new ItemKey("key1"));
            pool.RegisterKey(new ItemKey("key2"));

            var acquiredItems = Enumerable
                .Range(0, 100)
                .SelectMany(n =>
                    {
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
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
                        var item1 = pool.Acquire();
                        var item2 = pool.Acquire();
                        pool.Good(item1.PoolKey);
                        pool.Good(item2.PoolKey);
                        pool.Release(item1);
                        pool.Release(item2);
                        return new[] {item1, item2};
                    })
                .GroupBy(x => x.PoolKey)
                .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ItemKey>.Default);

            Assert.That(acquireFromDeadNodeCount, Is.InRange(0, 20));
            Assert.That(!reacquiredItems.ContainsKey(new ItemKey("key2")));
        }

        [Test]
        public void TestAcquireNewWithDeadNodes()
        {
            var pool = new ReplicaSetPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x) {IsAlive = false}));
            pool.RegisterKey(new ItemKey("key1"));
            pool.RegisterKey(new ItemKey("key2"));

            Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire());
            Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire());
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

            public string Value { get; set; }
        }

        private class Item : IDisposable, IPoolKeyContainer<ItemKey>, ILiveness
        {
            public Item(ItemKey key)
            {
                PoolKey = key;
                IsAlive = true;
            }

            public void Dispose()
            {
            }

            public bool IsAlive { get; set; }
            public ItemKey PoolKey { get; private set; }
        }
    }
}