namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class Column
    {
        public string Name { get; set; }
        public byte[] Value { get; set; }
        public int? TTL { get; set; }
        public long? Timestamp { get; set; }
    }
}