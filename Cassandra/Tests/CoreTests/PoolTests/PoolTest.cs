using System;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;
using SKBKontur.Cassandra.CassandraClient.Core.GenericPool.Exceptions;

namespace Cassandra.Tests.CoreTests.PoolTests
{
    [TestFixture]
    public class PoolTest
    {
        [Test]
        public void AcquireItemFromEmptyPool()
        {
            Item lastFactoryResult = null;
            var factoryInvokeCount = 0;

            using(var pool = new Pool<Item>(x =>
                {
                    factoryInvokeCount++;
                    return lastFactoryResult = new Item();
                }))
            {
                var item = pool.Acquire();
                Assert.That(item, Is.EqualTo(lastFactoryResult));
                Assert.That(factoryInvokeCount, Is.EqualTo(1));
            }
        }

        [Test]
        public void DisposeAndReleaseDeadItemsThroughAcquire()
        {
            using(var pool = new Pool<Item>(x => new Item()))
            {
                var item1 = pool.Acquire();
                item1.IsAlive = false;
                pool.Release(item1);
                var item2 = pool.Acquire();
                Assert.That(item2, Is.Not.EqualTo(item1));
                Assert.That(item1.Disposed);
            }
        }

        [Test]
        public void DisposeAndReleaseDeadItemsThroughAcquireExists()
        {
            using(var pool = new Pool<Item>(x => new Item()))
            {
                var item1 = pool.Acquire();
                item1.IsAlive = false;
                pool.Release(item1);
                Item item2;
                Assert.That(!pool.TryAcquireExists(out item2));                
                Assert.That(item1.Disposed);
            }
        }

        [Test]
        public void AcquireAndReleaseItemFromEmptyPool()
        {
            var factoryInvokeCount = 0;

            using(var pool = new Pool<Item>(x =>
                {
                    factoryInvokeCount++;
                    return new Item();
                }))
            {
                var item1 = pool.Acquire();
                pool.Release(item1);
                var item2 = pool.Acquire();

                Assert.That(item2, Is.EqualTo(item1));
                Assert.That(factoryInvokeCount, Is.EqualTo(1));
            }
        }

        [Test]
        public void TryReleaseItemTwice()
        {
            using(var pool = new Pool<Item>(x => new Item()))
            {
                var item1 = pool.Acquire();
                pool.Release(item1);
                Assert.Throws<FailedReleaseItemException>(() => pool.Release(item1));
            }
        }

        [Test]
        public void DisposeItemsAfterPoolDisposed()
        {
            Item item1;
            Item item2;
            using(var pool = new Pool<Item>(x => new Item()))
            {
                item1 = pool.Acquire();
                item2 = pool.Acquire();
                pool.Release(item1);
                pool.Release(item2);
                item1 = pool.Acquire();
            }
            Assert.That(item1.Disposed);
            Assert.That(item2.Disposed);
        }

        [Test]
        public void TestAcquireNew()
        {
            using(var pool = new Pool<Item>(x => new Item()))
            {
                var item1 = pool.AcquireNew();
                pool.Release(item1);
                var item2 = pool.AcquireNew();
                Assert.That(item2, Is.Not.EqualTo(item1));
            }
        }

        [Test]
        public void TestAcquireExists()
        {
            using(var pool = new Pool<Item>(x => new Item()))
            {
                var item1 = pool.AcquireNew();
                pool.Release(item1);
                Item item2;
                Assert.That(pool.TryAcquireExists(out item2));
                Assert.That(item2, Is.EqualTo(item1));
                Item item3;
                Assert.That(!pool.TryAcquireExists(out item3));
            }
        }

        [Test]
        public void MultiThreadTest()
        {
            using(var pool = new Pool<Item>(x => new Item()))
            {
                var threads = Enumerable
                    .Range(0, 100)
                    .Select(n => (Action)(() =>
                        {
                            // ReSharper disable AccessToDisposedClosure
                            var random = new Random(n * DateTime.UtcNow.Millisecond);
                            for(var i = 0; i < 100; i++)
                            {
                                var item = pool.Acquire();
                                try
                                {
                                    Assert.That(!item.IsUse);
                                    Assert.That(!item.Disposed);
                                    var item2 = pool.Acquire();
                                    try
                                    {
                                        Assert.That(!item2.IsUse);
                                        Assert.That(!item2.Disposed);
                                        item2.Use(TimeSpan.FromMilliseconds(random.Next(100)));
                                    }
                                    finally
                                    {
                                        pool.Release(item2);
                                    }
                                    item.Use(TimeSpan.FromMilliseconds(random.Next(100)));
                                    Assert.That(!item.IsUse);
                                    Assert.That(!item.Disposed);
                                }
                                finally
                                {
                                    pool.Release(item);
                                }
                                Thread.Sleep(TimeSpan.FromMilliseconds(random.Next(100)));
                            }
                            // ReSharper restore AccessToDisposedClosure
                        }))
                    .Select(x => new Thread(() => x()))
                    .ToList();
                threads.ForEach(x => x.Start());
                threads.ForEach(x => x.Join());
                Console.WriteLine(pool.TotalCount);
            }
        }

        private class Item : IDisposable, ILiveness
        {
            public Item()
            {
                IsAlive = true;
            }

            public void Dispose()
            {
                Disposed = true;
            }

            public void Use(TimeSpan timeout)
            {
                IsUse = true;
                try
                {
                    Thread.Sleep(timeout);
                }
                finally
                {
                    IsUse = false;
                }
            }

            public bool IsUse { get; private set; }
            public bool Disposed { get; private set; }
            public bool IsAlive { get; set; }
        }
    }
}