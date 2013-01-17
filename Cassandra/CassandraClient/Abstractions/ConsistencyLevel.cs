using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public enum ConsistencyLevel
    {
        ALL,
        ANY,
        EACH_QUORUM,
        LOCAL_QUORUM,
        ONE,
        QUORUM
    }

    public static class ConsistencyLevelExtensions
    {
        public static Apache.Cassandra.ConsistencyLevel ToThriftConsistencyLevel(this ConsistencyLevel consistencyLevel)
        {
            return (Apache.Cassandra.ConsistencyLevel)Enum.Parse(typeof(Apache.Cassandra.ConsistencyLevel), consistencyLevel.ToString());
        }
    }
}