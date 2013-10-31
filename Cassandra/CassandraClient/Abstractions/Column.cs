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
            if(column == null)
                return null;
            var result = new Apache.Cassandra.Column
                {
                    Name = StringExtensions.StringToBytes(column.Name),
                    Value = column.Value,
                    Timestamp = column.Timestamp ?? DateTimeService.UtcNow.Ticks,
                };
            if(column.TTL.HasValue)
                result.Ttl = column.TTL.Value;
            return result;
        }

        public static Column FromCassandraColumn(this Apache.Cassandra.Column cassandraColumn)
        {
            if(cassandraColumn == null)
                return null;
            var result = new Column();
            if(cassandraColumn.__isset.ttl)
                result.TTL = cassandraColumn.Ttl;
            result.Name = StringExtensions.BytesToString(cassandraColumn.Name);
            result.Timestamp = cassandraColumn.Timestamp;
            result.Value = cassandraColumn.Value;
            return result;
        }
    }
}