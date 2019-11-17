using SkbKontur.Cassandra.TimeBasedUuid;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    public class TimeBasedColumn
    {
        public TimeGuid Name { get; set; }
        public byte[] Value { get; set; }
        public long? Timestamp { get; set; }
        public int? Ttl { get; set; }
    }
}