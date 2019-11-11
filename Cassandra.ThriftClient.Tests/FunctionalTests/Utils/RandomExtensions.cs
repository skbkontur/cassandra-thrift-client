using System;

using JetBrains.Annotations;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils
{
    public static class RandomExtensions
    {
        [NotNull]
        public static byte[] NextBytes([NotNull] this Random random, int length)
        {
            var buf = new byte[length];
            random.NextBytes(buf);
            return buf;
        }

        public static ushort NextUshort([NotNull] this Random random, ushort minValue, ushort maxValue)
        {
            return (ushort)random.Next(minValue, maxValue);
        }

        public static TimeSpan NextTimeSpan([NotNull] this Random random, TimeSpan maxValue)
        {
            return TimeSpan.FromTicks((long)(random.NextDouble() * maxValue.Ticks));
        }
    }
}