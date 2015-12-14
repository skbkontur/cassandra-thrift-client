using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class RawIndexExpression
    {
        public byte[] ColumnName { get; set; }
        public IndexOperator IndexOperator { get; set; }
        public byte[] Value { get; set; }
    }

    public class IndexExpression
    {
        public string ColumnName { get; set; }
        public IndexOperator IndexOperator { get; set; }
        public byte[] Value { get; set; }
    }

    internal static class IndexExpressionExtensions
    {
        public static RawIndexExpression ToRawIndesExpression(this IndexExpression indexExpression)
        {
            return new RawIndexExpression
                {
                    ColumnName = StringExtensions.StringToBytes(indexExpression.ColumnName),
                    IndexOperator = indexExpression.IndexOperator,
                    Value = indexExpression.Value
                };
        }

        public static Apache.Cassandra.IndexExpression ToCassandraIndexExpression(this RawIndexExpression rawIndexExpression)
        {
            return new Apache.Cassandra.IndexExpression
                {
                    Column_name = rawIndexExpression.ColumnName,
                    Value = rawIndexExpression.Value,
                    Op = rawIndexExpression.IndexOperator.ToCassandraIndexOperator()
                };
        }
    }
}