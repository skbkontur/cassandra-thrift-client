using System;
using System.Diagnostics;
using System.Threading;

using NUnit.Framework;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils
{
    public static class Waiter
    {
        public static void Wait(Func<bool> stopWaiting, TimeSpan timeout)
        {
            var stopwatch = Stopwatch.StartNew();
            while (stopwatch.Elapsed < timeout)
            {
                if (stopWaiting())
                    return;
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            Assert.Fail($"Waiting timeout {timeout} expired");
        }
    }
}