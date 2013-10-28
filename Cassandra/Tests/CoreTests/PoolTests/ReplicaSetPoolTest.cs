using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;

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
            using(var pool = CreateReplicaSetPool(0))
                Assert.Throws<EmptyPoolException>(() => pool.Acquire(new ItemKey("key1")));
        }

        [Test]
        public void TestAcquireWithItemKeyIsNull()
        {
            using(var pool = CreateReplicaSetPool())
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
            using(var pool = CreateReplicaSetPool())
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
            using(var pool = CreateReplicaSetPool())
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
            using(var pool = CreateReplicaSetPool())
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
            using(var pool = CreateReplicaSetPool())
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
            using(var pool = CreateReplicaSetPool())
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
            using(var pool = CreateReplicaSetPool(2))
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
            using(var pool = CreateReplicaSetPool(2))
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
        public void TestAliveAfterDead()
        {
            const int itemCount = 100;
            const int attemptCount = 40;

            using(var pool = CreateReplicaSetPool(2))
            {
                var itemKey = new ItemKey("key1");
                Enumerable.Range(0, 100).ToList().ForEach(x => pool.BadReplica(new ReplicaKey("replica2"))); // Health: 0.01

                var acquiredItems = Enumerable
                    .Range(0, itemCount)
                    .Select(n => pool.Acquire(itemKey))
                    .ToList();

                var acquiredItemCount = acquiredItems
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(acquiredItemCount[new ReplicaKey("replica1")], Is.InRange(0.95 * itemCount, itemCount));
                int count;
                Assert.That(acquiredItemCount.TryGetValue(new ReplicaKey("replica2"), out count) ? count : 0, Is.InRange(0.00 * itemCount, 0.05 * itemCount));

                acquiredItems.ForEach(pool.Release);

                var reacquiredItems = Enumerable
                    .Range(0, attemptCount)
                    .SelectMany(n =>
                        {
                            var items = Enumerable.Range(0, itemCount).Select(i => pool.Acquire(itemKey)).ToList();
                            items.ForEach(pool.Good);
                            items.ForEach(pool.Release);
                            return items;
                        })
                    .GroupBy(x => x.ReplicaKey)
                    .ToDictionary(x => x.Key, x => x.Count(), EqualityComparer<ReplicaKey>.Default);

                Assert.That(reacquiredItems[new ReplicaKey("replica1")], Is.InRange(0.53 * itemCount * attemptCount, 0.60 * itemCount * attemptCount));
                Assert.That(reacquiredItems[new ReplicaKey("replica2")], Is.InRange(0.40 * itemCount * attemptCount, 0.47 * itemCount * attemptCount));
            }
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
        public void TestCreationItemsOnDeadReplica()
        {
            const int attemptCount = 100;
            const int itemCount = 100;
            var deadNodeAttemptCount = 0;

            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>(
                (x, r) => r.Name == "replica2" ?
                              new Pool<Item>(y =>
                                  {
                                      deadNodeAttemptCount++;
                                      return new Item(x, r) {IsAlive = false};
                                  }) :
                              new Pool<Item>(y => new Item(x, r))))
            {
                var itemKey = new ItemKey("key1");
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

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

            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>(
                (x, r) => r.Name == "replica2" ?
                              new Pool<Item>(y =>
                                  {
                                      deadNodeAttemptCount++;
                                      return new Item(x, r) {IsAlive = false};
                                  }) :
                              new Pool<Item>(y => new Item(x, r))))
            {
                Enumerable.Range(0, 100).ToList().ForEach(x => pool.BadReplica(new ReplicaKey("replica2"))); // Health: 0.01

                var itemKey = new ItemKey("key1");
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

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
            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, r) =>
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
                }))
            {
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
        }

        [Test]
        public void TestAcquireNewWithDeadNodes()
        {
            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) => new Pool<Item>(y => new Item(x, z) {IsAlive = false})))
            {
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

                Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire(new ItemKey("1")));
                Assert.Throws<AllItemsIsDeadExceptions>(() => pool.Acquire(new ItemKey("1")));
            }
        }

        [Test]
        public void TestAcquireConnectionWithExceptionInOnePool()
        {
            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) => new Pool<Item>(y =>
                {
                    if (z.Name == "replica1")
                        throw new Exception("FakeException");
                    return new Item(x, z);
                })))
            {
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

                for(int i = 0; i < 1000; i++)
                {
                    var item = pool.Acquire(new ItemKey("1"));
                    Assert.That(item.ReplicaKey.Name, Is.EqualTo("replica2"));
                }
            }
        }

        [Test]
        public void TestTryAcquireConnectionWithExceptionAllPools()
        {
            using (var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) =>
                {
                    return new Pool<Item>(y => { throw new Exception("FakeException"); });
                }))
            {
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

                for (int i = 0; i < 1000; i++)
                {
                    try
                    {
                        pool.Acquire(new ItemKey("1"));
                        Assert.Fail();
                    }
                    catch(Exception exception)
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
            using(var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) => new Pool<Item>(y => new Item(x, z)), TimeSpan.FromMilliseconds(100)))
            {
                pool.RegisterReplica(new ReplicaKey("replica1"));
                pool.RegisterReplica(new ReplicaKey("replica2"));

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

        private static ReplicaSetPool<Item, ItemKey, ReplicaKey> CreateReplicaSetPool(int replicaCount = 1, string nameFormat = "replica{0}")
        {
            var pool = ReplicaSetPool.Create<Item, ItemKey, ReplicaKey>((x, z) => new Pool<Item>(y => new Item(x, z)));
            Enumerable
                .Range(1, replicaCount)
                .Select(n => string.Format(nameFormat, n))
                .Select(x => new ReplicaKey(x))
                .ToList()
                .ForEach(pool.RegisterReplica);
            return pool;
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
                Disposed = true;
            }

            public bool IsAlive { get; set; }
            public ItemKey PoolKey { get; private set; }
            public ReplicaKey ReplicaKey { get; private set; }
            public bool Disposed { get; private set; }
        }
    }
}