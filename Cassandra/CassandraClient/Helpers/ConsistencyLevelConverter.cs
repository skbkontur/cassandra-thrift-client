using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class ConsistencyLevelConverter
    {
        public static AquilesConsistencyLevel ToAquilesConsistencyLevel(this ConsistencyLevel consistencyLevel)
        {
            return (AquilesConsistencyLevel)Enum.Parse(typeof(AquilesConsistencyLevel), consistencyLevel.ToString());
        }
    }
}