using CassandraClient.Abstractions;
using CassandraClient.AquilesTrash.Encoders;
using CassandraClient.AquilesTrash.Model;

namespace CassandraClient.Helpers
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