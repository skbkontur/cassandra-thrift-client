namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class IndexExpression
    {
        public string ColumnName { get; set; }
        public IndexOperator IndexOperator { get; set; }
        public byte[] Value { get; set; }
    }
}