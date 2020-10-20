using System;
using System.Linq;
using System.Threading;

using Moq;

using NUnit.Framework;

using SkbKontur.Cassandra.ThriftClient.Core.GenericPool;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions;
using SkbKontur.Cassandra.ThriftClient.Core.Metrics;
using SkbKontur.Cassandra.TimeBasedUuid;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Tests.UnitTests.CoreTests.PoolTests
{
    [TestFixture]
    public class PoolTest
    {
        [Test]
        public void AcquireItemFromEmptyPool()
        {
            Item lastFactoryResult = null;
            var factoryInvokeCount = 0;

            using (var pool = new Pool<Item>(x =>
                {
                    factoryInvokeCount++;
                    return lastFactoryResult = new Item();
                }, NoOpMetrics.Instance, new SilentLog()))
            {
                var item = pool.Acquire();
                Assert.That(item, Is.EqualTo(lastFactoryResult));
                Assert.That(factoryInvokeCount, Is.EqualTo(1));
            }
        }

        [Test]
        public void DisposeAndReleaseDeadItemsThroughAcquire()
        {
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
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
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
            {
                var item1 = pool.Acquire();
                item1.IsAlive = false;
                pool.Release(item1);
                Assert.That(!pool.TryAcquireExists(out _));
                Assert.That(item1.Disposed);
            }
        }

        [Test]
        public void AcquireAndReleaseItemFromEmptyPool()
        {
            var factoryInvokeCount = 0;

            using (var pool = new Pool<Item>(x =>
                {
                    factoryInvokeCount++;
                    return new Item();
                }, NoOpMetrics.Instance, new SilentLog()))
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
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
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
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
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
            var metricsMock = new Mock<IPoolMetrics>(MockBehavior.Strict);
            metricsMock.Setup(x => x.RecordAcquireNewConnection()).Verifiable();
            using (var pool = new Pool<Item>(x => new Item(), metricsMock.Object, new SilentLog()))
            {
                var item1 = pool.AcquireNew();
                pool.Release(item1);
                var item2 = pool.AcquireNew();
                Assert.That(item2, Is.Not.EqualTo(item1));
                metricsMock.Verify(x => x.RecordAcquireNewConnection(), Times.Exactly(2));
            }
        }

        [Test]
        public void TestAcquireExists()
        {
            var metricsMock = new Mock<IPoolMetrics>(MockBehavior.Strict);
            metricsMock.Setup(x => x.RecordAcquireNewConnection()).Verifiable();
            using (var pool = new Pool<Item>(x => new Item(), metricsMock.Object, new SilentLog()))
            {
                var item1 = pool.AcquireNew();
                pool.Release(item1);
                Assert.That(pool.TryAcquireExists(out var item2));
                Assert.That(item2, Is.EqualTo(item1));
                Assert.That(!pool.TryAcquireExists(out _));
                metricsMock.Verify(x => x.RecordAcquireNewConnection(), Times.Exactly(1));
            }
        }

        [Test]
        public void TestRemoveItemFromPool()
        {
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
            {
                var item1 = pool.Acquire();
                var item2 = pool.Acquire();
                pool.Release(item1);
                pool.Remove(item2);

                var item3 = pool.Acquire();
                var item4 = pool.Acquire();
                Assert.That(item3, Is.EqualTo(item1));
                Assert.That(item4, Is.Not.EqualTo(item1) & Is.Not.EqualTo(item2));
            }
        }

        [Test]
        public void TestTryRemoveReleasedItemFromPool()
        {
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
            {
                var item1 = pool.Acquire();
                var item2 = pool.Acquire();
                pool.Release(item1);
                pool.Release(item2);
                Assert.Throws<RemoveFromPoolFailedException>(() => pool.Remove(item2));
            }
        }

        [Test]
        public void TestTryRemoveItemDoesNotBelongInPool()
        {
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
            {
                var item1 = pool.Acquire();
                pool.Release(item1);
                Assert.Throws<RemoveFromPoolFailedException>(() => pool.Remove(new Item()));
            }
        }

        [Test]
        public void MultiThreadTest()
        {
            using (var pool = new Pool<Item>(x => new Item(), NoOpMetrics.Instance, new SilentLog()))
            {
                var threads = Enumerable
                    .Range(0, 100)
                    .Select(n => (Action)(() =>
                        {
                            // ReSharper disable AccessToDisposedClosure
                            for (var i = 0; i < 100; i++)
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
                                        item2.Use(TimeSpan.FromMilliseconds(ThreadLocalRandom.Instance.Next(100)));
                                    }
                                    finally
                                    {
                                        pool.Release(item2);
                                    }
                                    item.Use(TimeSpan.FromMilliseconds(ThreadLocalRandom.Instance.Next(100)));
                                    Assert.That(!item.IsUse);
                                    Assert.That(!item.Disposed);
                                }
                                finally
                                {
                                    pool.Release(item);
                                }
                                Thread.Sleep(TimeSpan.FromMilliseconds(ThreadLocalRandom.Instance.Next(100)));
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

            public bool IsAlive { get; set; }

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
        }
    }
}