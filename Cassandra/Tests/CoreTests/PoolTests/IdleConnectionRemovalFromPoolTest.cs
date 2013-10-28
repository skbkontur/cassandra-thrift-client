using System;
using System.Linq;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Core.GenericPool;

namespace Cassandra.Tests.CoreTests.PoolTests
{
    [TestFixture]
    public class IdleConnectionRemovalFromPoolTest
    {
        [Test]
        public void TestRemoveConnection()
        {
            using(var pool = new Pool<Item>(x => new Item()))
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
        public void TestRemoveConnectionMultiThread()
        {
            var creationCount = 0;
            using(var pool = new Pool<Item>(x =>
                {
                    Interlocked.Increment(ref creationCount);
                    return new Item();
                }))
            {
                var items = Enumerable.Range(0, 10000).ToList().Select(x => pool.Acquire()).ToList();
                items.ForEach(pool.Release);
                Thread.Sleep(21);
                creationCount = 0;

                const int threadCount = 100;
                var threads = Enumerable
                    .Range(0, threadCount)
                    .Select(n => (ThreadStart)(() =>
                        {
                            for(var i = 0; i < 3000; i++)
                            {
                                var random = new Random(n);
                                var item = pool.Acquire();
                                Thread.Sleep(random.Next(10));
                                pool.Release(item);    
                            }                            
                        }))
                    .Select(x => new Thread(x))
                    .ToList();

                var removeThread = new Thread(() =>
                    {
                        for(var i = 0; i < 1000; i++)
                        {
                            Thread.Sleep(10);
                            pool.RemoveIdleItems(TimeSpan.FromMilliseconds(20));
                        }
                    });

                threads.ForEach(x => x.Start());
                removeThread.Start();

                removeThread.Join();
                threads.ForEach(x => x.Join());

                Assert.That(creationCount, Is.EqualTo(0));
                Assert.That(pool.TotalCount, Is.LessThanOrEqualTo(threadCount));
                Assert.That(pool.FreeItemCount, Is.LessThanOrEqualTo(threadCount));
                Assert.That(pool.BusyItemCount, Is.EqualTo(0));

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