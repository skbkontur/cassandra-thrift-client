using System;
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
                Thread.Sleep(TimeSpan.FromMilliseconds(101));
                var item3 = pool.Acquire();
                pool.Release(item3);
                var count = pool.RemoveIdleItem(TimeSpan.FromMilliseconds(100));
                
                var item4 = pool.Acquire();
                var item5 = pool.Acquire();

                Assert.That(count, Is.EqualTo(1));
                Assert.That(item4, Is.EqualTo(item3));
                Assert.That(item5, Is.Not.EqualTo(item1) & Is.Not.EqualTo(item2) & Is.Not.EqualTo(item3));
                Assert.That(item2.Disposed);
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