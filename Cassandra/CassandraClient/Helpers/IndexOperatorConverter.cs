using System;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class IndexOperatorConverter
    {
        public static AquilesIndexOperator ToAquilesIndexOperator(this IndexOperator indexOperator)
        {
            return (AquilesIndexOperator)Enum.Parse(typeof(AquilesIndexOperator), indexOperator.ToString());
        }
    }
}