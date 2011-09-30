using System;
using System.Runtime.InteropServices;

namespace CassandraClient.Core
{
    public static class DateTimeService
    {
        public static DateTime UtcNow { get { return new DateTime(GetUtcTicks(), DateTimeKind.Utc); } }

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
            long currentCounter = GetCounter();
            double elapsed = (double)(currentCounter - startCounter) / frequency;
            if(elapsed > 1.0)
            {
                initialized = false;
                return GetUtcTicks();
            }
            return startTicks + (long)(elapsed * 10000000 + 0.5);
        }

        private static readonly long frequency = GetFrequency();

        [ThreadStatic]
        private static bool initialized;

        [ThreadStatic]
        private static long startCounter;

        [ThreadStatic]
        private static long startTicks;
    }
}