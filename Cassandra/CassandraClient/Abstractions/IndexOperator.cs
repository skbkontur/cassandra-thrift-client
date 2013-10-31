using System;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    // ReSharper disable InconsistentNaming
    public enum IndexOperator
    {
        EQ,
        GTE,
        GT,
        LTE,
        LT
    }

    // ReSharper restore InconsistentNaming

    internal static class IndexOperatorConverter
    {
        public static Apache.Cassandra.IndexOperator ToCassandraIndexOperator(this IndexOperator indexOperator)
        {
            return (Apache.Cassandra.IndexOperator)Enum.Parse(typeof(Apache.Cassandra.IndexOperator), indexOperator.ToString());
        }
    }
}