using System;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    // ReSharper disable InconsistentNaming
    public enum ConsistencyLevel
    {
        ALL,
        ANY,
        EACH_QUORUM,
        LOCAL_QUORUM,
        ONE,
        QUORUM
    }

    // ReSharper restore InconsistentNaming

    internal static class ConsistencyLevelExtensions
    {
        public static Apache.Cassandra.ConsistencyLevel ToThriftConsistencyLevel(this ConsistencyLevel consistencyLevel)
        {
            return (Apache.Cassandra.ConsistencyLevel)Enum.Parse(typeof(Apache.Cassandra.ConsistencyLevel), consistencyLevel.ToString());
        }
    }
}