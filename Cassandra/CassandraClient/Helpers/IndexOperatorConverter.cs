using System;

using Aquiles.Model;

using CassandraClient.Abstractions;

namespace CassandraClient.Helpers
{
    public static class IndexOperatorConverter
    {
        public static AquilesIndexOperator ToAquilesIndexOperator(this IndexOperator indexOperator)
        {
            return (AquilesIndexOperator)Enum.Parse(typeof(AquilesIndexOperator), indexOperator.ToString());
        }
    }
}