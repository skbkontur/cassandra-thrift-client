using System;
using System.Runtime.InteropServices;
using System.Threading;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal static class DateTimeService
    {
        public static DateTime UtcNow
        {
            get
            {
                var cand = new DateTime(GetUtcTicks(), DateTimeKind.Utc);
                if(cand < lastReturned)
                    cand = lastReturned;
                lastReturned = cand;
                return cand;
            }
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool QueryPerformanceFrequency(out long lpFrequency);

        [DllImport("kernel32.dll", SetLastError = true)]
        private static extern bool QueryPerformanceCounter(out long lpPerformanceCount);

        private static long GetCounter()
        {
            long counter;
            if(!QueryPerformanceCounter(out counter))
                throw new InvalidOperationException("Cannot get performance counter!");
            return counter;
        }

        private static long GetFrequency()
        {
            long result;
            if(!QueryPerformanceFrequency(out result))
                throw new InvalidOperationException("Cannot get performance frequency!");
            return result;
        }

        private static void Init()
        {
            startCounter = GetCounter();
            startTicks = DateTime.UtcNow.Ticks;
            initialized = true;
        }

        private static long GetUtcTicks()
        {
            if(!initialized) Init();
            double elapsed = (double)(GetCounter() - startCounter) / frequency;
            if(elapsed > 1.0)
            {
                //Тут происходят действия, направленные на то, чтобы функция UtcNow была неубывающей
                lock(lockObject)
                {
                    elapsed = (double)(GetCounter() - startCounter) / frequency;
                    if(elapsed > 1.0)
                    {
                        initialized = false;
                        Thread.Sleep(10);
                        return GetUtcTicks();
                    }
                }
            }
            return startTicks + (long)(elapsed * 10000000 + 0.5);
        }

        private static readonly object lockObject = new object();

        private static readonly long frequency = GetFrequency();

        [ThreadStatic]
        private static bool initialized;

        [ThreadStatic]
        private static long startCounter;

        [ThreadStatic]
        private static long startTicks;

        [ThreadStatic]
        private static DateTime lastReturned;
    }
}