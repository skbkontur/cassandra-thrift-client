using System;

using CassandraClient.Abstractions;
using CassandraClient.AquilesTrash.Model;

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