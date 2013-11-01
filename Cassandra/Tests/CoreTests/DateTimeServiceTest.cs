using System;
using System.Threading;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Core;

namespace Cassandra.Tests.CoreTests
{
    public class DateTimeServiceTest : TestBase
    {
        [Test]
        public void TestPrecision()
        {
            var last = DateTimeService.UtcNow.Ticks;
            var start = DateTime.UtcNow;
            var count = 0;
            do
            {
                for(var i = 0; i < 1000000; ++i)
                {
                    var cur = DateTimeService.UtcNow.Ticks;
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
            var last = DateTimeService.UtcNow.Ticks;
            var start = DateTime.UtcNow;
            do
            {
                for(var i = 0; i < 1000000; ++i)
                {
                    var cur = DateTimeService.UtcNow.Ticks;
                    Assert.That(cur >= last, string.Format("cur={0}\r\n last={1}", cur, last));
                    last = cur;
                }
            } while(DateTime.UtcNow - start < TimeSpan.FromSeconds(10));
        }

        [Test]
        public void TestReturnsUtc()
        {
            var maxExpectedDiff = CalculateDateTimeDiff() * 4;
            Console.WriteLine("Max expected diff: {0}", maxExpectedDiff);

            long maxDiff = 0;
            var start = DateTime.UtcNow;
            do
            {
                for(var i = 0; i < 1000000; ++i)
                {
                    var cur = DateTimeService.UtcNow;
                    var actual = DateTime.UtcNow;
                    var diff = Math.Abs(cur.Ticks - actual.Ticks);
                    maxDiff = Math.Max(maxDiff, diff);
                }
                Console.WriteLine(maxDiff);
            } while(DateTime.UtcNow - start < TimeSpan.FromSeconds(5));
            Console.WriteLine(maxDiff);
            Assert.That(maxDiff, Is.LessThanOrEqualTo(maxExpectedDiff));
        }

        [Test]
        public void TestReturnsUtcLongTest()
        {
            var maxExpectedDiff = CalculateDateTimeDiff() * 4;
            Console.WriteLine("Max expected diff: {0}", maxExpectedDiff);

            long maxDiff = 0;
            var start = DateTime.UtcNow;
            do
            {
                for(var i = 0; i < 1000000; ++i)
                {
                    var cur = DateTimeService.UtcNow;
                    var actual = DateTime.UtcNow;
                    var diff = Math.Abs(cur.Ticks - actual.Ticks);
                    maxDiff = Math.Max(maxDiff, diff);
                }
                Console.WriteLine(maxDiff);
            } while(DateTime.UtcNow - start < TimeSpan.FromSeconds(100));
            Console.WriteLine(maxDiff);
            Assert.That(maxDiff, Is.LessThanOrEqualTo(maxExpectedDiff));
        }

        private static long CalculateDateTimeDiff()
        {
            long result = 0;
            var previousNow = DateTime.UtcNow;
            for(var i = 0; i < 10000; i++)
            {
                var currentNow = DateTime.UtcNow;
                Thread.Sleep(1);
                var diff = currentNow.Ticks - previousNow.Ticks;
                previousNow = currentNow;

                if(result < diff)
                    result = diff;
            }
            Console.WriteLine(result);
            return result;
        }
    }
}