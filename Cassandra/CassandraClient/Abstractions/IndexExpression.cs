using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class GeneralIndexExpression
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
        public static GeneralIndexExpression ToGeneralIndesExpression(this IndexExpression indexExpression)
        {
            return new GeneralIndexExpression
                {
                    ColumnName = StringExtensions.StringToBytes(indexExpression.ColumnName),
                    IndexOperator = indexExpression.IndexOperator,
                    Value = indexExpression.Value
                };
        }

        public static Apache.Cassandra.IndexExpression ToCassandraIndexExpression(this GeneralIndexExpression generalIndexExpression)
        {
            return new Apache.Cassandra.IndexExpression
                {
                    Column_name = generalIndexExpression.ColumnName,
                    Value = generalIndexExpression.Value,
                    Op = generalIndexExpression.IndexOperator.ToCassandraIndexOperator()
                };
        }
    }
}