using SKBKontur.Cassandra.CassandraClient.Core;
using SKBKontur.Cassandra.CassandraClient.Helpers;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class Column
    {
        public string Name { get; set; }
        public byte[] Value { get; set; }
        public int? TTL { get; set; }
        public long? Timestamp { get; set; }
    }

    internal static class ColumnExtensions
    {
        public static Apache.Cassandra.Column ToCassandraColumn(this Column column)
        {
            var result = new Apache.Cassandra.Column
                {
                    Name = StringHelpers.StringToBytes(column.Name),
                    Value = column.Value,
                    Timestamp = column.Timestamp ?? DateTimeService.UtcNow.Ticks,
                };
            if (column.TTL.HasValue)
                result.Ttl = column.TTL.Value;
            return result;
        }
    }
}