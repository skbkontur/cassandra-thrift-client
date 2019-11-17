using System;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool
{
    public class PoolSettings
    {
        public double DeadHealth { get; set; }
        public TimeSpan MaxCheckInterval { get; set; }
        public TimeSpan CheckIntervalIncreaseBasis { get; set; }

        public static PoolSettings CreateDefault()
        {
            return new PoolSettings
                {
                    DeadHealth = 0.01,
                    CheckIntervalIncreaseBasis = TimeSpan.FromSeconds(1),
                    MaxCheckInterval = TimeSpan.FromMinutes(1)
                };
        }
    }
}