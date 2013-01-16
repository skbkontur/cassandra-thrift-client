using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public enum IndexOperator
    {
        EQ,
        GTE,
        GT,
        LTE,
        LT
    }

    internal static class IndexOperatorConverter
    {
        public static Apache.Cassandra.IndexOperator ToCassandraIndexOperator(this Abstractions.IndexOperator indexOperator)
        {
            return (Apache.Cassandra.IndexOperator)Enum.Parse(typeof(Apache.Cassandra.IndexOperator), indexOperator.ToString());
        }
    }
}