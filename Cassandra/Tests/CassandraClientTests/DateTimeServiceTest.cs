using System;

using CassandraClient.Core;

using NUnit.Framework;

namespace Cassandra.Tests.CassandraClientTests
{
    public class DateTimeServiceTest : TestBase
    {
        [Test]
        public void TestPrecision()
        {
            long last = DateTimeService.UtcNow.Ticks;
            DateTime start = DateTime.UtcNow;
            int count = 0;
            do
            {
                for(int i = 0; i < 10000000; ++i)
                {
                    long cur = DateTimeService.UtcNow.Ticks;
                    if(cur != last)
                    {
                        last = cur;
                        ++count;
                    }
                }
            } while(DateTime.UtcNow - start < TimeSpan.FromSeconds(5));
            Assert.That(count > 5000000);
        }

        [Test]
        public void TestReturnsUtc()
        {
            long maxDiff = 0;
            DateTime start = DateTime.UtcNow;
            do
            {
                for(int i = 0; i < 1000000; ++i)
                {
                    DateTime cur = DateTimeService.UtcNow;
                    DateTime actual = DateTime.UtcNow;
                    long diff = Math.Abs(cur.Ticks - actual.Ticks);
                    maxDiff = Math.Max(maxDiff, diff);
                }
                Console.WriteLine(maxDiff);
            } while (DateTime.UtcNow - start < TimeSpan.FromSeconds(5));
            Console.WriteLine(maxDiff);
            Assert.That(maxDiff < 20000);
        }
    }
}