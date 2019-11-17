namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class RawColumn
    {
        public byte[] Name { get; set; }
        public byte[] Value { get; set; }
        public int? TTL { get; set; }
        public long? Timestamp { get; set; }
    }
}