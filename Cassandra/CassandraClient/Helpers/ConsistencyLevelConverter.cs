using System;

using CassandraClient.Abstractions;
using CassandraClient.AquilesTrash.Command;

namespace CassandraClient.Helpers
{
    public static class ConsistencyLevelConverter
    {
        public static AquilesConsistencyLevel ToAquilesConsistencyLevel(this ConsistencyLevel consistencyLevel)
        {
            return (AquilesConsistencyLevel)Enum.Parse(typeof(AquilesConsistencyLevel), consistencyLevel.ToString());
        }
    }
}