using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Encoders;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class IndexExpressionConverter
    {
        public static AquilesIndexExpression ToAquilesIndexExpression(this IndexExpression indexExpression)
        {
            return new AquilesIndexExpression
                {
                    ColumnName = ByteEncoderHelper.UTF8Encoder.ToByteArray(indexExpression.ColumnName),
                    IndexOperator = indexExpression.IndexOperator.ToAquilesIndexOperator(),
                    Value = indexExpression.Value
                };
        }
    }
}