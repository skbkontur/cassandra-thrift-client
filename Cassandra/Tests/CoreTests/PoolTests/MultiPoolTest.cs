using System;
using System.Collections.Generic;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;

namespace Cassandra.Tests.CoreTests.PoolTests
{
    [TestFixture]
    public class MultiPoolTest
    {
        [Test]
        public void TestAcquireWithoutRegisteredKeys()
        {
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));            
            Assert.Throws<EmptyPoolException>( () => pool.Acquire());
        }

        [Test]
        public void TestAcquireItem()
        {
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            var item1 = pool.Acquire();
            pool.Release(item1);
            var item2 = pool.Acquire();
            Assert.That(item2, Is.EqualTo(item1));
        }

        [Test]
        public void TestReleaseItemOfNotRegisterPool()
        {
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            var item1 = pool.Acquire();
            pool.Release(item1);
            var item2 = new Item(new ItemKey("key2"));
            Assert.Throws<InvalidPoolKeyException>(() => pool.Release(item2));
        }

        [Test]
        public void TestReleaseItemNotAcquiredFromPool()
        {
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
            pool.RegisterKey(new ItemKey("key1"));
            var item1 = pool.Acquire();
            pool.Release(item1);
            var item2 = new Item(new ItemKey("key1"));
            Assert.Throws<FailedReleaseItemException>(() => pool.Release(item2));
        }

        [Test]
        public void TestAcquireItemFromKeysWithHealthNodes()
        {
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
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
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
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
            var pool = new MultiPool<Item, ItemKey>(x => new Pool<Item>(y => new Item(x)));
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

        private class Item : IDisposable, IPoolKeyContainer<ItemKey>
        {
            public Item(ItemKey key)
            {
                PoolKey = key;
            }

            public void Dispose()
            {
            }

            public ItemKey PoolKey { get; private set; }
        }
    }
}