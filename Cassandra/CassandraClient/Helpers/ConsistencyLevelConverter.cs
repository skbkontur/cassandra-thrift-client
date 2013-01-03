using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

using ApacheConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class ConsistencyLevelConverter
    {
        public static ApacheConsistencyLevel ToThriftConsistencyLevel(this ConsistencyLevel consistencyLevel)
        {
            return (ApacheConsistencyLevel)Enum.Parse(typeof(ApacheConsistencyLevel), consistencyLevel.ToString());
        }
    }
}