using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;

using Vostok.Logging.Logs;

namespace Cassandra.ThriftClient.Tests.UnitTests.CoreTests.PoolTests
{
    [TestFixture]
    public class IdleConnectionRemovalFromPoolTest
    {
        [Test]
        public void TestRemoveConnection()
        {
            using(var pool = new Pool<Item>(x => new Item(), new SilentLog()))
            {
                var item1 = pool.Acquire();
                var item2 = pool.Acquire();
                pool.Release(item2);
                pool.Release(item1);
                Thread.Sleep(TimeSpan.FromMilliseconds(150));
                var item3 = pool.Acquire();
                pool.Release(item3);
                var count = pool.RemoveIdleItems(TimeSpan.FromMilliseconds(100));

                var item4 = pool.Acquire();
                var item5 = pool.Acquire();

                Assert.That(count, Is.EqualTo(1));
                Assert.That(item4, Is.EqualTo(item3));
                Assert.That(item5, Is.Not.EqualTo(item1) & Is.Not.EqualTo(item2) & Is.Not.EqualTo(item3));
                Assert.That(item2.Disposed);
            }
        }

        [Test]
        [SuppressMessage("ReSharper", "AccessToDisposedClosure")]
        [SuppressMessage("ReSharper", "AccessToModifiedClosure")]
        public void TestRemoveConnectionMultiThread()
        {
            var newPoolItemsCreatedCount = 0;
            using(var pool = new Pool<Item>(x =>
                {
                    Interlocked.Increment(ref newPoolItemsCreatedCount);
                    return new Item();
                }, new SilentLog()))
            {
                const int threadCount = 100;
                const int initialPoolItemsCount = threadCount - 1;
                var items = Enumerable.Range(0, initialPoolItemsCount).ToList().Select(x => pool.Acquire()).ToList();
                items.ForEach(pool.Release);
                Thread.Sleep(21);
                Assert.That(pool.TotalCount, Is.EqualTo(initialPoolItemsCount));
                Assert.That(pool.FreeItemCount, Is.EqualTo(initialPoolItemsCount));
                Assert.That(pool.BusyItemCount, Is.EqualTo(0));

                const int acquisitionsPerThreadCount = 1000;
                newPoolItemsCreatedCount = 0;
                var threads = Enumerable
                    .Range(0, threadCount)
                    .Select(n => (ThreadStart)(() =>
                        {
                            var rng = new Random(n);
                            for(var i = 0; i < acquisitionsPerThreadCount; i++)
                            {
                                var item = pool.Acquire();
                                Thread.Sleep(rng.Next(10));
                                pool.Release(item);
                            }
                        }))
                    .Select(x => new Thread(x))
                    .ToList();

                var minIdleTimeSpan = TimeSpan.FromMilliseconds(20);
                var stopSignal = new ManualResetEventSlim();
                var removedIdleItemsCount = 0;
                var removeThread = new Thread(() =>
                    {
                        while(!stopSignal.Wait(TimeSpan.FromMilliseconds(10)))
                            removedIdleItemsCount += pool.RemoveIdleItems(minIdleTimeSpan);
                    });

                removeThread.Start();
                threads.ForEach(x => x.Start());

                threads.ForEach(x => x.Join());
                Assert.That(newPoolItemsCreatedCount, Is.GreaterThan(0), "invalid newPoolItemsCreatedCount");
                Assert.That(removedIdleItemsCount, Is.GreaterThan(0), "invalid removedIdleItemsCount");

                Thread.Sleep(minIdleTimeSpan + minIdleTimeSpan);
                stopSignal.Set();
                removeThread.Join();

                Assert.That(removedIdleItemsCount, Is.EqualTo(initialPoolItemsCount + newPoolItemsCreatedCount), "invalid removedIdleItemsCount");
                Assert.That(pool.TotalCount, Is.EqualTo(0), "invalid pool.TotalCount");
                Assert.That(pool.FreeItemCount, Is.EqualTo(0), "invalid pool.FreeItemCount");
                Assert.That(pool.BusyItemCount, Is.EqualTo(0), "invalid pool.BusyItemCount");
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

            public bool IsAlive { get; }

            public bool Disposed { get; private set; }
        }
    }
}