using System;

using SKBKontur.Cassandra.CassandraClient.Core;

using NUnit.Framework;

namespace Cassandra.Tests.CoreTests
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
                for(int i = 0; i < 1000000; ++i)
                {
                    long cur = DateTimeService.UtcNow.Ticks;
                    if(cur != last)
                    {
                        last = cur;
                        ++count;
                    }
                }
            } while(DateTime.UtcNow - start < TimeSpan.FromSeconds(1));
            Assert.That(count > 1000000);
        }

        [Test]
        public void TestAscending()
        {
            long last = DateTimeService.UtcNow.Ticks;
            DateTime start = DateTime.UtcNow;
            do
            {
                for(int i = 0; i < 1000000; ++i)
                {
                    long cur = DateTimeService.UtcNow.Ticks;
                    Assert.That(cur >= last, string.Format("cur={0}\r\n last={1}", cur, last));
                    last = cur;
                }
            } while(DateTime.UtcNow - start < TimeSpan.FromSeconds(10));
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
            Assert.That(maxDiff < 50000);
        }
        
        [Test]
        public void TestReturnsUtcLongTest()
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
            } while (DateTime.UtcNow - start < TimeSpan.FromSeconds(100));
            Console.WriteLine(maxDiff);
            Assert.That(maxDiff < 50000);
        }
    }
}